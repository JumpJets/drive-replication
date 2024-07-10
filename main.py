"""
# Drive replication

Copy all the files and folders, with preserving metadata such as dates, hidden attributes, etc.

## `progress.jsonl` structure:
1. {source: Path, destination: Path, exclude: deque[str]}
2. hardlinks: dict[Path, deque[Path]]
3. extra_attrib_dirs: deque[Path]
4. junction_dirs: deque[Path]
5. {dirs: deque[Path], list_files: deque[Path], list_links: deque[Path]}
6. (one per line) processed: deque[Path]
"""

from collections import deque
from collections.abc import Sequence
import contextlib
import fnmatch
from itertools import islice
import os
from pathlib import Path
from shutil import Error as ShutilError
from shutil import copy2, copystat, copytree
import stat
from subprocess import run
import sys

import orjson
from pydantic import BaseModel, Field
from rich import inspect
from rich.console import Console
from rich.live import Live
from rich.markup import escape
from rich.progress import BarColumn, DownloadColumn, MofNCompleteColumn, Progress, TaskID, TransferSpeedColumn
from rich.status import Status as ConsoleStatus
from rich.table import Table

IS_WINDOWS = sys.platform == "win32"

if IS_WINDOWS:
    import ctypes

    from win32file import FindFileNames


# ? Const


INVALID_FILE_ATTRIBUTES = -1
TRACKED_ATTRIBUTES = (
    stat.FILE_ATTRIBUTE_ARCHIVE
    # | stat.FILE_ATTRIBUTE_COMPRESSED # NOTE: require DeviceIoControl with FSCTL_SET_COMPRESSION
    # | stat.FILE_ATTRIBUTE_ENCRYPTED # NOTE: for new files, otherwise use EncryptFile function
    | stat.FILE_ATTRIBUTE_HIDDEN
    | stat.FILE_ATTRIBUTE_READONLY
    | stat.FILE_ATTRIBUTE_SYSTEM
)

WINDOWS_DEFAULT_EXCLUDE: list[str] = [
    "hiberfil.sys",  # ? RAM on disc for hybernation
    "pagefile.sys",  # ? RAM on disc, temporary file
    "System Volume Information",  # ? Drive-specific temporary files
    # f"System Volume Information{os.sep}*",
]

progress_file = (Path(__file__) / ".." / "progress.jsonl").resolve()
console = Console()


# ? Model


class ProgressContext(BaseModel):
    """
    Progress bar context
    """

    main_progress: Progress = Field(..., title="Main progress bar")
    files: TaskID = Field(..., title="Files progress bar ID")
    dirs: TaskID = Field(..., title="Directories progress bar ID")
    links: TaskID = Field(..., title="Links progress bar ID")
    sub_progress: Progress = Field(..., title="Sub progress bar for file size")
    size: TaskID = Field(..., title="Size progress bar ID")
    table: Table = Field(..., title="Table for main and sub progress bar")

    model_config = ConfigDict(arbitrary_types_allowed=True)


class Context(BaseModel):
    """
    Context for replication
    """

    source: Path = Field(..., title="Source directory")
    len_source_parts: int = Field(..., title="Source parts length")
    destination: Path = Field(..., title="Destination directory")
    exclude: deque[str] = Field(..., title="Folders to exclude")

    total_dirs: int = Field(default=1, title="Sum of total directories", description="Root directory is counted as well")
    list_dirs: deque[str] = Field(default_factory=deque, title="Listing all directories")
    total_files: int = Field(default=0, title="Sum ot total files", description="For progress bar")
    total_size: int = Field(default=0, title="Sum of total files size", description="For progress bar")
    sizes: dict[str, int] = Field(default={}, title="Sizes of individual files", description="For progress bar")
    list_files: deque[str] = Field(default_factory=deque, title="Listing all files")
    total_links: int = Field(default=0, title="Sum of total links", description="Hardlinks on 2nd+ occurance, symlinks, junction points")
    list_links: deque[str] = Field(default_factory=deque, title="Listing all links", description="Hardlinks on 2nd+ occurance, symlinks, junction points")
    current_dir: int = Field(default=0, title="Currend directory index")
    current_file: int = Field(default=0, title="Current file index")

    # ? Metadata that is not being restored:
    hardlinks: dict[Path, deque[Path]] = Field(default={}, title="Collection of hardlinks", description="Key is first occurrance, value is list of 2nd+ occurrances")
    extra_attrib_dirs: deque[Path] = Field(default_factory=deque, title="List of folders with extra attributes", description="Archive, hidden, readonly, system attributes")
    junction_dirs: deque[Path] = Field(default_factory=deque, title="List of junction directories", description="Require different execution to create")
    oserrored: deque[Path] = Field(default_factory=deque, title="List of paths which was unable to replicate")

    processed: deque[str] = Field(default_factory=deque, title="Processed files", description="For dumping session")


class Context_Progressed(Context):
    """
    Context for replication with progress
    """

    progress: ProgressContext = Field(default=None, title="Progress bar context")


# ? Util


def confirm(text: str = "", /) -> bool:
    """
    Confirm dialog (case insensitive)

    Y / y / YES / Yes / yes / TRUE / True / true / 1
    """

    out = input(text).strip().lower()
    return out in {"y", "yes", "true", "1"}


def init_progress(ctx: Context, /) -> Context_Progressed:
    """
    Init progress bar
    """

    _default_columns = Progress.get_default_columns()
    progress = Progress(_default_columns[0], BarColumn(None), MofNCompleteColumn(), _default_columns[-1], expand=True)
    progress_files = progress.add_task("  Files:", total=ctx.total_files)
    progress_dirs = progress.add_task("Folders:", total=ctx.total_dirs)
    progress_links = progress.add_task("  Links:", total=ctx.total_links)
    progress.advance(progress_dirs, advance=-1)

    sub_progress = Progress(_default_columns[0], BarColumn(None), TransferSpeedColumn(), DownloadColumn(), _default_columns[-1], expand=True)
    progress_size = sub_progress.add_task("   Size:", total=ctx.total_size)

    progress_table = Table.grid()
    progress_table.add_row(sub_progress)
    progress_table.add_row(progress)

    return Context_Progressed(
        **ctx.model_dump(),
        progress=ProgressContext(
            main_progress=progress,
            files=progress_files,
            dirs=progress_dirs,
            links=progress_links,
            sub_progress=sub_progress,
            size=progress_size,
            table=progress_table,
        ),
    )


def has_hidden_attribute(path_stat: os.stat_result, /) -> bool:
    """
    Check file or directory for hidden attribute
    """

    return path_stat.st_file_attributes & stat.FILE_ATTRIBUTE_HIDDEN == stat.FILE_ATTRIBUTE_HIDDEN


def has_tracked_attributes(path_stat: os.stat_result, /) -> bool:
    """
    Check file or directory for tracked attributes
    """

    return path_stat.st_file_attributes & TRACKED_ATTRIBUTES != 0


def is_hardlinked(path: Path, path_stat: os.stat_result, /) -> bool:
    """
    Check if file have > 1 hardlinks
    """

    return path.is_file() and path_stat.st_nlink > 1 and not path.is_symlink()


def are_hardlinked(p1: Path, p2: Path, /) -> bool:
    """
    Check if two files are hardlinked
    """

    if not p1.is_file() or not p2.is_file():
        return False

    return p1.samefile(p2) or (((p1s := p1.stat()).st_ino == (p2s := p2.stat()).st_ino) and (p1s.st_dev == p2s.st_dev))


def list_hardlinks_windows(p: Path | str | bytes, /) -> deque[Path]:
    """
    Return list of hardlinks for a given file (Windows)
    """

    return deque(Path(f).absolute() for f in FindFileNames(os.fsdecode(os.fspath(p))))


def create_junction_point(source: Path, destination: Path, /) -> int:
    """
    Create Junction Point (reparse point)

    Only NTFS supports Junction Points
    """

    if not source.is_dir():
        msg = "Source is not directory"
        raise OSError(msg)
    if destination.exists():
        msg = "Destination is not empty"
        raise OSError(msg)

    return run(["cmd", "/c", "mklink", "/J", destination, source], check=True).returncode  # noqa: S603, S607


# ? Supplimentary replication functions
# * Collecting metadata about file or directory


def collect_metadata_hardlinks(ctx: Context, path: Path, /, path_stat: os.stat_result) -> bool:
    """
    Collect metadata for hardlinks
    """

    if (path_is_hardlinked := is_hardlinked(path, path_stat)) and not any(path in hl for hl in ctx.hardlinks.values()):
        _hardlinks = list_hardlinks_windows(path) if IS_WINDOWS else deque((path,))  # TODO: add Linux detection
        ctx.hardlinks[path] = _hardlinks
        ctx.exclude.extend(str(hl) for hl in _hardlinks if hl != path)

    return path_is_hardlinked


def collect_metadata_tracked_attributes(
    ctx: Context,
    path: Path,
    str_path: str,
    path_stat: os.stat_result,
    *,
    path_is_hardlinked: bool,
    is_junction: bool,
    is_dir: bool = False,
) -> bool:
    """
    Collect metadata for tracked attributes (Windows)

    Return False when should be aborted
    """

    try:  # ? Broken symlinks can't get attributes, raise FileNotFoundError | (is_hidden := )
        if (IS_WINDOWS and has_tracked_attributes(path_stat)) and is_dir:
            ctx.extra_attrib_dirs.append(path)
    except FileNotFoundError:
        if path_is_hardlinked:
            _hardlinks = ctx.hardlinks[path]
            for hl in _hardlinks:
                ctx.exclude.remove(str(hl))
            del ctx.hardlinks[path]

        if is_junction:
            ctx.junction_dirs.remove(path)
            ctx.exclude.remove(str_path)

        return False

    except OSError:
        if path_is_hardlinked:
            _hardlinks = ctx.hardlinks[path]
            for hl in _hardlinks:
                ctx.exclude.remove(str(hl))
            del ctx.hardlinks[path]

        if is_junction:
            ctx.junction_dirs.remove(path)
            ctx.exclude.remove(str_path)

        ctx.oserrored.append(path)
        console.print_exception(max_frames=1)

        return False

    return True


def collect_metadata_assign_path_type(
    ctx: Context,
    path: Path,
    str_path: str,
    path_stat: os.stat_result,
    *,
    path_is_hardlinked: bool,
    is_symlink_or_junction: bool,
    is_dir: bool = False,
) -> None:
    """
    Assign path type to context
    """

    match is_dir, is_symlink_or_junction, path_is_hardlinked:
        case True, False, _:
            ctx.total_dirs += 1
            ctx.list_dirs.append(str_path)
        case True, True, _:
            ctx.total_links += 1
            ctx.list_links.append(str_path)
        case False, False, False:
            ctx.total_files += 1
            ctx.list_files.append(str_path)
            ctx.total_size += path_stat.st_size
            ctx.sizes[str(path)] = path_stat.st_size
        case False, _, True:
            if path in ctx.hardlinks:
                ctx.total_files += 1
                ctx.list_files.append(str_path)
                ctx.total_size += path_stat.st_size
                ctx.sizes[str(path)] = path_stat.st_size
            else:
                ctx.total_links += 1
                ctx.list_links.append(str_path)
                ctx.sizes[str(path)] = path_stat.st_size
        case False, True, _:
            ctx.total_links += 1
            ctx.list_links.append(str_path)
            ctx.sizes[str(path)] = path_stat.st_size


def collect_metadata(ctx: Context, path: Path, /, is_dir: bool = False) -> None:
    """
    Collect metadata that is not restored by shutil

    * hardlinks treated as files, this make it creating duplication instead of creating hardlinks
        NOTE: detection for hardlinks rely on Windows win32 function, Linux alternative is not implemented
    * folders didn't restore attributes like hidden, system, etc
    * junction points created as regular folders and all content in them are duplicated
    """

    # NOTE: pattern matching is very slow here, uses fnmatch.translate a lot of the time
    # if any(path.match(e) for e in exclude):
    #     return

    str_path = str(path)

    # NOTE: faster alternative w/o pattern matching
    if any(e in str_path for e in ctx.exclude):
        return

    path_stat: os.stat_result = path.stat(follow_symlinks=False)

    try:
        path_is_hardlinked = collect_metadata_hardlinks(ctx, path, path_stat)
    except OSError:
        ctx.oserrored.append(path)
        console.print_exception(max_frames=1)
        return

    is_symlink = path.is_symlink()
    is_junction = path.is_junction()
    is_symlink_or_junction = is_symlink or is_junction
    if is_junction:
        ctx.junction_dirs.append(path)
        ctx.exclude.append(str_path)

    if not collect_metadata_tracked_attributes(ctx, path, str_path, path_stat, path_is_hardlinked=path_is_hardlinked, is_junction=is_junction, is_dir=is_dir):
        return

    collect_metadata_assign_path_type(ctx, path, str_path, path_stat, path_is_hardlinked=path_is_hardlinked, is_symlink_or_junction=is_symlink_or_junction, is_dir=is_dir)


# * Scan directory for files


def _scan_windows_dir(ctx: Context, /, status: ConsoleStatus) -> None:
    """
    Scan directory (Windows)
    """

    for root, dirs, files in ctx.source.walk(top_down=True, follow_symlinks=False):
        for name in dirs:
            collect_metadata(ctx, root / name, is_dir=True)

            ctx.current_dir += 1
            status.update(f"Scanning... Dirs: [bold blue]{ctx.current_dir}[/bold blue] Files: [bold blue]{
                ctx.current_file}[/bold blue] | [yellow]{escape(str(root / name))}[/yellow]")

        for name in files:
            collect_metadata(ctx, root / name)

            ctx.current_file += 1
            status.update(f"Scanning... Dirs: [bold blue]{ctx.current_dir}[/bold blue] Files: [bold blue]{
                ctx.current_file}[/bold blue] | [yellow]{escape(str(root / name))}[/yellow]")

        # NOTE: while this is more performant, some directories take too long to update, so moving update to loops above
        # current_dir += len(dirs)
        # current_file += len(files)
        # status.update(f"Scanning... Dirs: [bold blue]{ctx.current_dir}[/bold blue] Files: [bold blue]{
        #     ctx.current_file}[/bold blue] | [yellow]{escape(str(root))}[/yellow]")


def _scan_linux_dir(ctx: Context, /, status: ConsoleStatus) -> None:
    """
    Scan directory (Linux)

    ? Exclude mount points on Linux systems
    """

    for root, dirs, files in ctx.source.walk(top_down=True, follow_symlinks=False):
        for name in dirs:
            if (root / name).is_mount():
                dirs[:] = [d for d in dirs if d != name]
                continue

            collect_metadata(ctx, root / name, is_dir=True)

            ctx.current_dir += 1
            status.update(f"Scanning... Dirs: [bold blue]{ctx.current_dir}[/bold blue] Files: [bold blue]{
                ctx.current_file}[/bold blue] | [yellow]{escape(str(root / name))}[/yellow]")

        for name in files:
            collect_metadata(ctx, root / name)

            ctx.current_file += 1
            status.update(f"Scanning... Dirs: [bold blue]{ctx.current_dir}[/bold blue] Files: [bold blue]{
                ctx.current_file}[/bold blue] | [yellow]{escape(str(root / name))}[/yellow]")


def scan_dir(ctx: Context, /) -> None:
    """
    Scan directory
    """

    with console.status("Scanning... Dirs: 0 Files: 0") as status:
        if IS_WINDOWS:
            _scan_windows_dir(ctx, status=status)
        else:
            _scan_linux_dir(ctx, status=status)


# * Copy files and directories


def copy_tree_ignore_controller(ctx: Context_Progressed, path: str, files: Sequence[str]) -> set[str]:
    """
    Ignore controller

    Argument `path` is a root to current working directory
    and `files` is a list of file names (without directory path) in that folder.
    Return would be an iterable of files to ignore.
    """

    temp_exclude: deque[str] = deque()
    p = Path(path)

    for e in ctx.exclude:
        pe = Path(e)
        last_part = pe.parts[-1]

        # ? Strict check (directory path is equal to excluded directory)
        # Example: path: D:\test, exc: D:\test or path: D:\test\sub, exc: D:\test
        if p.is_relative_to(e):
            temp_exclude.extend(files)

        # ? Pattern matching (with *)
        # Example: path: D:\test with some files and exc: D:\test\*.txt
        elif p.match(e):
            temp_exclude.extend(fnmatch.filter(files, last_part))  # TODO: replace for faster alternative (fnmatch.translate is slow)

        # ? Files matching
        # Example: exc: D:\test\1.txt, path: D:\test, files ["1.txt", "2.txt"]
        elif pe.parent == p and (_files := fnmatch.filter(files, last_part)):  # TODO: same
            temp_exclude.extend(_files)

    # progress.console.print(f"\nProcessing path: {path!r} Files:", files, "Path excluded:", path in exclude, _exclude)
    # input()
    to_be_processed = [(p / f) for f in files if f not in temp_exclude]
    ctx.processed.extend(map(str, to_be_processed))
    ctx.progress.main_progress.advance(ctx.progress.dirs, advance=1)
    # NOTE: moved to copy2 function
    # progress.advance(progress_files, advance=len([f for f in to_be_processed if f.is_file()]))

    return set(temp_exclude)


def copy_tree_copy_controller(ctx: Context_Progressed, source: str, destination: str, *, follow_symlinks: bool = True) -> str:
    """
    Copy controller

    For every file this function would be called with absolute paths for `source` and `destination`.
    Folders does not created with this function.
    """

    ctx.progress.main_progress.advance(ctx.progress.files, advance=1)
    ctx.progress.sub_progress.advance(ctx.progress.size, advance=ctx.sizes[source])
    # ctx.progress.main_progress.console.print(f'Copying: "{escape(source)}"') # → "{destination}"

    return copy2(source, destination, follow_symlinks=follow_symlinks)


def copy_tree(ctx: Context_Progressed, /, dir_exist_ok: bool = True) -> None:
    """
    Copy tree with progress
    """

    def _ignore_controller(path: str, files: Sequence[str]) -> set[str]:
        nonlocal ctx
        return copy_tree_ignore_controller(ctx, path, files)

    def _copy_controller(source: str, destination: str, *, follow_symlinks: bool = True) -> str:
        nonlocal ctx
        return copy_tree_copy_controller(ctx, source, destination, follow_symlinks=follow_symlinks)

    try:
        copytree(ctx.source, ctx.destination, symlinks=True, ignore=_ignore_controller, copy_function=_copy_controller, ignore_dangling_symlinks=True, dirs_exist_ok=dir_exist_ok)
    except FileExistsError as e:
        inspect(e)
    except ShutilError as e:
        error_files = {n: er for n, _, er in e.args[0]}

        # inspect(tuple(error_files.items()))
        ctx.progress.main_progress.advance(ctx.progress.files, advance=-len(error_files))
        ctx.progress.main_progress.console.print("Errors during file or directory copy:", style="red")

        for f, er in error_files.items():
            ctx.progress.main_progress.console.print(f"{f}:", er, style="red")
            with contextlib.suppress(ValueError):
                ctx.processed.remove(f)

        ctx.progress.main_progress.console.print()
    except KeyboardInterrupt:
        ctx.progress.main_progress.console.print("Cancelling task. You can restart program and continue from latest file.", style="blue")

        with progress_file.open(mode="a", encoding="utf-8") as f:
            f.writelines(p + "\n" for p in islice(ctx.processed, len(ctx.processed) - 1))

        raise


# * Apply attributes, create links


def apply_windows_attributes(ctx: Context_Progressed, /, kernel32: ctypes.WinDLL) -> None:
    """
    Apply Windows attributes (NTFS)
    """

    for folder in ctx.extra_attrib_dirs:
        attrs = folder.stat(follow_symlinks=False).st_file_attributes
        target = Path(ctx.destination, *folder.parts[ctx.len_source_parts :])

        if kernel32.SetFileAttributesW(str(target), attrs):
            ctx.progress.main_progress.console.print(f"Copy stat from {folder!r} to {target!r}: {attrs}")
            # ctx.progress.main_progress.advance(progress_links, advance=1)
        else:
            ctx.oserrored.append(folder)
            ctx.progress.main_progress.console.print(f"Copy stat error at {target!r}", style="red")

        # if (attrs := kernel32.GetFileAttributesW(str(target))) == INVALID_FILE_ATTRIBUTES:
        #     raise ctypes.WinError(ctypes.get_last_error())
        #     continue

        # attrs |= stat.FILE_ATTRIBUTE_HIDDEN
        # if not kernel32.SetFileAttributesW(target, attrs):
        #     raise ctypes.WinError(ctypes.get_last_error())

    ctx.progress.main_progress.console.print()


def create_junction_dirs(ctx: Context_Progressed, /, kernel32: ctypes.WinDLL) -> None:
    """
    Create junction directories (Windows / NTFS)
    """

    for folder in ctx.junction_dirs:
        attrs = folder.stat(follow_symlinks=False).st_file_attributes
        folder_source = folder.readlink()
        target_source = Path(ctx.destination, *folder_source.parts[ctx.len_source_parts :])
        target = Path(ctx.destination, *folder.parts[ctx.len_source_parts :])
        # target = Path(f"{os.sep * 2}?{os.sep}{ctx.destination.parts[0]}", *ctx.destination.parts[1:], *folder_source.parts[len_source:])

        try:
            create_junction_point(target_source, target)
            copystat(folder, target)
            kernel32.SetFileAttributesW(str(target), attrs)

            ctx.progress.main_progress.console.print(f"Created Junction point from {target_source!r} to {target!r}")
            ctx.progress.main_progress.advance(ctx.progress.links, advance=1)
        except OSError as e:
            ctx.oserrored.append(folder)
            ctx.progress.main_progress.console.print("Junction creation error:", e, f"at {target!r}", style="red")


def create_hardlinks_with_attributes(ctx: Context_Progressed, /, kernel32: ctypes.WinDLL) -> None:
    """
    Create hardlinks with attributes (Windows / NTFS)
    """

    for _hl_source, hl_list in ctx.hardlinks.items():
        for _hl in hl_list:
            if _hl == _hl_source:
                continue

            try:
                hl_source = Path(ctx.destination, *_hl_source.parts[ctx.len_source_parts :])
                hl = Path(ctx.destination, *_hl.parts[ctx.len_source_parts :])
                hl.hardlink_to(hl_source)

                attrs = _hl.stat(follow_symlinks=False).st_file_attributes
                kernel32.SetFileAttributesW(str(hl), attrs)

                ctx.progress.main_progress.advance(ctx.progress.links, advance=1)
                ctx.progress.main_progress.console.print(f"Created hardlink from {hl_source!r} to {hl!r}")
            except FileExistsError as e:
                ctx.progress.main_progress.console.print("Hardlink creation error:", e)

    ctx.progress.main_progress.console.print()


# * Write progress to JSON


def write_progress_file_metadata(ctx: Context, /) -> None:
    """
    Write progress file metadata
    """

    with progress_file.open(mode="ab") as f:
        f.write(orjson.dumps({"source": str(ctx.source), "destination": str(ctx.destination), "exclude": list(ctx.exclude)}))
        f.write(b"\n")
        f.write(orjson.dumps({str(k): [str(p) for p in v] for k, v in ctx.hardlinks.items()}))
        f.write(b"\n")
        f.write(orjson.dumps([str(p) for p in ctx.extra_attrib_dirs]))
        f.write(b"\n")
        f.write(orjson.dumps([str(p) for p in ctx.junction_dirs]))
        f.write(b"\n")
        f.write(orjson.dumps({"dirs": list(ctx.list_dirs), "files": list(ctx.list_files), "links": list(ctx.list_links)}))
        f.write(b"\n")


def write_progress_file_processed(ctx: Context, /) -> None:
    """
    Write progress file processed
    """

    with progress_file.open(mode="a", encoding="utf-8") as f:
        f.writelines(p + "\n" for p in ctx.processed)


def write_progress_file_oserrored(ctx: Context, /) -> None:
    """
    Write progress file oserrored
    """

    if not ctx.oserrored:
        return

    with progress_file.with_suffix(".error.json").open(mode="ab") as f:
        f.write(orjson.dumps([str(e) for e in ctx.oserrored]))


# ? Main replication function


def replication(source: Path, destination: Path, exclude: deque[str] | None = None, *, dir_exist_ok: bool = True) -> bool:
    """
    Drive replication
    """

    ctx: Context | Context_Progressed = Context(
        source=source,
        destination=destination,
        exclude=(
            deque(source.drive + os.sep + ex for ex in WINDOWS_DEFAULT_EXCLUDE) if not exclude else (exclude + deque(source.drive + os.sep + ex for ex in WINDOWS_DEFAULT_EXCLUDE))
        )
        if IS_WINDOWS
        else (exclude or deque()),
        len_source_parts=len(source.parts),
    )

    console.print("Initial settings for drive / folder replication:", style="bold white")
    console.print(f"     Source: [bold blue]{escape(str(ctx.source))}[/bold blue]")
    console.print(f"Destination: [bold blue]{escape(str(ctx.destination))}[/bold blue]")
    console.print(f"    Exclude: {ctx.exclude!r}\n")
    console.print("Continue? (Ctrl+C to exit)", style="italic")
    input()

    console.print("Collecting extra metadata", style="bold white")

    # //──────────────────────────────────────────────────────────

    scan_dir(ctx)

    write_progress_file_metadata(ctx)

    console.print("Hardlinks:", len(ctx.hardlinks) or "None")
    console.print("Hidden:", ctx.extra_attrib_dirs or "None")
    if ctx.junction_dirs:
        console.print("Junction:", ctx.junction_dirs or "None")
    console.print("Exclude:", ctx.exclude)
    console.print("Total  dirs:", ctx.total_dirs)
    console.print("Total files:", ctx.total_files)
    console.print("Total links:", ctx.total_links)
    console.print(f"Total size: {ctx.total_size / (1024 ** 3):.2f} GB, {ctx.total_size} bytes")
    if ctx.oserrored:
        console.print("OS Errors:", ctx.oserrored, style="red")
    console.print("\n──────────────────────────────────────────────────────────\n")
    console.print("Metadata collected. Start replication? (Ctrl+C to cancel)", style="italic")
    input()

    # //──────────────────────────────────────────────────────────

    ctx = init_progress(ctx)

    with Live(ctx.progress.table, refresh_per_second=10):
        copy_tree(ctx, dir_exist_ok=dir_exist_ok)

        # //──────────────────────────────────────────────────────────

        if IS_WINDOWS and (ctx.extra_attrib_dirs or ctx.junction_dirs):
            kernel32 = ctypes.WinDLL("kernel32.dll", use_last_error=True)

            apply_windows_attributes(ctx, kernel32=kernel32)
            create_junction_dirs(ctx, kernel32=kernel32)
            create_hardlinks_with_attributes(ctx, kernel32=kernel32)

        # TODO: else Linux hardlinks

        ctx.progress.main_progress.advance(ctx.progress.dirs, advance=1)

    write_progress_file_processed(ctx)

    write_progress_file_oserrored(ctx)

    # console.print("\nProcessed:")
    # console.print(processed)
    console.print("Replication completed", style="bold green")

    return not ctx.oserrored


def replication_continue(
    source: Path,
    destination: Path,
    exclude: deque[str],
    hardlinks: dict[Path, list[Path]],
    extra_attrib_dirs: deque[Path],
    junction_dirs: deque[Path],
    list_dirs: deque[Path],
    list_files: deque[Path],
    list_links: deque[Path],
    processed: deque[Path],
) -> None:
    """
    Continuation of disrupted progress
    """

    raise NotImplementedError  # TODO: implement


# ? Startup


def main() -> None:  # noqa: C901
    """
    Init replication
    """

    args = sys.argv[1:]
    exclude = deque(args[2:])

    try:
        if progress_file.exists() and progress_file.stat().st_size > 0:
            console.print("Found previous session. Resume? (Y/yes/true/1 to continue)", style="italic")
            if confirm("> "):
                with progress_file.open(mode="rb") as f:
                    source_dest_exclude = orjson.loads(f.readline())
                    source, dest, exclude = Path(source_dest_exclude["source"]), Path(source_dest_exclude["destination"]), source_dest_exclude["exclude"]

                    hardlinks = {Path(k): [Path(p) for p in v] for k, v in orjson.loads(f.readline()).items()}
                    extra_attrib_dirs = deque(Path(p) for p in orjson.loads(f.readline()))
                    junction_dirs = deque(Path(p) for p in orjson.loads(f.readline()))
                    all_files = orjson.loads(f.readline())  # * {"dirs": [], "files": [], "links": []}

                    processed: deque[Path] = deque()
                    for p in f.readlines():
                        processed.append(Path(p.decode("utf-8")))

                replication_continue(
                    source=source,
                    destination=dest,
                    exclude=exclude,
                    hardlinks=hardlinks,
                    extra_attrib_dirs=extra_attrib_dirs,
                    junction_dirs=junction_dirs,
                    list_dirs=all_files["dirs"],
                    list_files=all_files["files"],
                    list_links=all_files["links"],
                    processed=processed,
                )
                sys.exit(0)

            print()

        # progress_file.unlink()
        with progress_file.open(mode="w") as f:
            pass

        if not args:
            console.print("Enter source drive or any other path.", style="bold white")  # \nWindows examples:\nD:\\\nC:\\MyFolder\n\nLinux examples:\n/dev/sda
            _source = input("> ").strip() or "."
            print()
        else:
            _source = args[0]

        if len(args) <= 1:
            console.print("Enter destination drive or any other path.", style="bold white")
            _destination = input("> ").strip()
            print()

            console.print("Optionally provide list of excluded folders. One per line. Wildcards can be used (e.g. *.sys)\n")
            while _exclude_path := input("> ").strip():
                exclude.append(_exclude_path)
            print()
        else:
            _destination = args[1]

        if not _destination:
            console.print("Destination is not specified. Exiting.", style="red")
            sys.exit(1)

        source = Path(_source).resolve()
        destination = Path(_destination).resolve()

        if not source.is_dir():
            console.print("Source drive or path is not exists. Exiting.", style="red")
            sys.exit(1)

        if destination.is_dir() and not any(destination.iterdir()):
            console.print("Target directory is not empty.", style="red")
            console.print("Confirm continue (files may be overwritten) (Y/yes/true/1)", style="italic")
            if not confirm("> "):
                sys.exit(1)

        replication(source, destination, exclude=exclude)
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    main()
