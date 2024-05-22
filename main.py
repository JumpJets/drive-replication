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
from rich import inspect
from rich.console import Console
from rich.live import Live
from rich.markup import escape
from rich.progress import BarColumn, DownloadColumn, MofNCompleteColumn, Progress, TransferSpeedColumn
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


# ? Util


def confirm(text: str = "", /) -> bool:
    """
    Confirm dialog (case insensitive)

    Y / y / YES / Yes / yes / TRUE / True / true / 1
    """

    out = input(text).strip().lower()
    return out in {"y", "yes", "true", "1"}


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


# ? Main replication function


def replication(source: Path, destination: Path, exclude: deque[str] | None = None, *, dir_exist_ok: bool = True) -> bool:  # noqa: C901
    """
    Drive replication
    """

    if not exclude:
        exclude = deque(source.drive + os.sep + ex for ex in WINDOWS_DEFAULT_EXCLUDE) if IS_WINDOWS else deque()
    elif IS_WINDOWS:
        exclude.extend(source.drive + os.sep + ex for ex in WINDOWS_DEFAULT_EXCLUDE)

    console.print("Initial settings for drive / folder replication:", style="bold white")
    console.print(f"     Source: [bold blue]{escape(str(source))}[/bold blue]")
    console.print(f"Destination: [bold blue]{escape(str(destination))}[/bold blue]")
    console.print(f"    Exclude: {exclude!r}\n")
    console.print("Continue? (Ctrl+C to exit)", style="italic")
    input()

    console.print("Collecting extra metadata", style="bold white")
    len_source = len(source.parts)
    total_dirs = 1  # ? + root dir
    list_dirs: deque[str] = deque()
    total_files = 0
    total_size = 0
    sizes: dict[str, int] = {}
    list_files: deque[str] = deque()
    total_links = 0
    list_links: deque[str] = deque()
    current_dir = 0
    current_file = 0

    # ? Metadata that is not being restored:
    hardlinks: dict[Path, deque[Path]] = {}
    extra_attrib_dirs: deque[Path] = deque()
    junction_dirs: deque[Path] = deque()
    oserrored: deque[Path] = deque()

    def collect_metadata(path: Path, /, is_dir: bool = False) -> None:  # noqa: C901
        """
        Collect metadata that is not restored by shutil

        * hardlinks treated as files, this make it creating duplication instead of creating hardlinks
          NOTE: detection for hardlinks rely on Windows win32 function, Linux alternative is not implemented
        * folders didn't restore attributes like hidden, system, etc
        * junction points created as regular folders and all content in them are duplicated
        """

        nonlocal exclude, total_dirs, total_files, total_size, total_links, hardlinks, extra_attrib_dirs, junction_dirs, oserrored, list_dirs, list_files, list_links, sizes

        # NOTE: pattern matching is very slow here, uses fnmatch.translate a lot of the time
        # if any(path.match(e) for e in exclude):
        #     return

        str_path = str(path)

        # NOTE: faster alternative w/o pattern matching
        if any(e in str_path for e in exclude):
            return

        path_stat: os.stat_result = path.stat(follow_symlinks=False)

        try:
            if (path_is_hardlinked := is_hardlinked(path, path_stat)) and not any(path in hl for hl in hardlinks.values()):
                _hardlinks = list_hardlinks_windows(path) if IS_WINDOWS else deque((path,))  # TODO: add Linux detection
                hardlinks[path] = _hardlinks
                exclude.extend(str(hl) for hl in _hardlinks if hl != path)
        except OSError:
            oserrored.append(path)
            console.print_exception(max_frames=1)
            return

        is_symlink = path.is_symlink()
        is_junction = path.is_junction()
        is_symlink_or_junction = is_symlink or is_junction
        if is_junction:
            junction_dirs.append(path)
            exclude.append(str_path)

        try:  # ? Broken symlinks can't get attributes, raise FileNotFoundError | (is_hidden := )
            if (IS_WINDOWS and has_tracked_attributes(path_stat)) and is_dir:
                extra_attrib_dirs.append(path)
        except FileNotFoundError:
            if path_is_hardlinked:
                _hardlinks = hardlinks[path]
                for hl in _hardlinks:
                    exclude.remove(str(hl))
                del hardlinks[path]

            if is_junction:
                junction_dirs.remove(path)
                exclude.remove(str_path)
            return

        except OSError:
            if path_is_hardlinked:
                _hardlinks = hardlinks[path]
                for hl in _hardlinks:
                    exclude.remove(str(hl))
                del hardlinks[path]

            if is_junction:
                junction_dirs.remove(path)
                exclude.remove(str_path)

            oserrored.append(path)
            console.print_exception(max_frames=1)

            return

        match is_dir, is_symlink_or_junction, path_is_hardlinked:
            case True, False, _:
                total_dirs += 1
                list_dirs.append(str_path)
            case True, True, _:
                total_links += 1
                list_links.append(str_path)
            case False, False, False:
                total_files += 1
                list_files.append(str_path)
                total_size += path_stat.st_size
                sizes[str(path)] = path_stat.st_size
            case False, _, True:
                if path in hardlinks:
                    total_files += 1
                    list_files.append(str_path)
                    total_size += path_stat.st_size
                    sizes[str(path)] = path_stat.st_size
                else:
                    total_links += 1
                    list_links.append(str_path)
                    sizes[str(path)] = path_stat.st_size
            case False, True, _:
                total_links += 1
                list_links.append(str_path)
                sizes[str(path)] = path_stat.st_size

        # console.print(f"path = {path!r}")
        # console.print("path.is_dir() =", is_dir)
        # console.print("path.is_symlink() =", is_symlink)
        # console.print("path.is_junction() =", is_junction)
        # console.print("path.readlink().absolute() =", path.readlink().absolute() if is_symlink_or_junction else None)
        # console.print("path.stat(follow_symlinks=False) =", path.stat(follow_symlinks=False))
        # console.print("path_is_hardlinked =", _path_is_hardlinked)
        # console.print("hardlinks.get(path) =", hardlinks.get(path))
        # console.print("is_hidden =", is_hidden, end="\n\n")
        # input()

    # //──────────────────────────────────────────────────────────

    with console.status("Scanning... Dirs: 0 Files: 0") as status:
        if IS_WINDOWS:
            for root, dirs, files in source.walk(top_down=True, follow_symlinks=False):
                for name in dirs:
                    collect_metadata(root / name, is_dir=True)

                    current_dir += 1
                    status.update(
                        f"Scanning... Dirs: [bold blue]{current_dir}[/bold blue] Files: [bold blue]{current_file}[/bold blue] | [yellow]{escape(str(root / name))}[/yellow]",
                    )

                for name in files:
                    collect_metadata(root / name)

                    current_file += 1
                    status.update(
                        f"Scanning... Dirs: [bold blue]{current_dir}[/bold blue] Files: [bold blue]{current_file}[/bold blue] | [yellow]{escape(str(root / name))}[/yellow]",
                    )

                # NOTE: while this is more performant, some directories take too long to update, so moving update to loops above
                # current_dir += len(dirs)
                # current_file += len(files)
                # status.update(f"Scanning... Dirs: [bold blue]{current_dir}[/bold blue] Files: [bold blue]{current_file}[/bold blue] | [yellow]{escape(str(root))}[/yellow]")

        else:  # ? Exclude mount points on Linux systems
            for root, dirs, files in source.walk(top_down=True, follow_symlinks=False):
                for name in dirs:
                    if (root / name).is_mount():
                        dirs[:] = [d for d in dirs if d != name]
                        continue

                    collect_metadata(root / name, is_dir=True)

                for name in files:
                    collect_metadata(root / name)

                current_dir += len(dirs)
                current_file += len(files)
                status.update(f"Scanning... Dirs: [bold blue]{current_dir}[/bold blue] Files: [bold blue]{current_file}[/bold blue] | [yellow]{escape(str(root))}[/yellow]")

    with progress_file.open(mode="ab") as f:
        f.write(orjson.dumps({"source": str(source), "destination": str(destination), "exclude": list(exclude)}))
        f.write(b"\n")
        f.write(orjson.dumps({str(k): [str(p) for p in v] for k, v in hardlinks.items()}))
        f.write(b"\n")
        f.write(orjson.dumps([str(p) for p in extra_attrib_dirs]))
        f.write(b"\n")
        f.write(orjson.dumps([str(p) for p in junction_dirs]))
        f.write(b"\n")
        f.write(orjson.dumps({"dirs": list(list_dirs), "files": list(list_files), "links": list(list_links)}))
        f.write(b"\n")

    console.print("Hardlinks:", len(hardlinks) or "None")
    console.print("Hidden:", extra_attrib_dirs or "None")
    if junction_dirs:
        console.print("Junction:", junction_dirs or "None")
    console.print("Exclude:", exclude)
    console.print("Total  dirs:", total_dirs)
    console.print("Total files:", total_files)
    console.print("Total links:", total_links)
    console.print(f"Total size: {total_size / (1024 ** 3):.2f} GB, {total_size} bytes")
    if oserrored:
        console.print("OS Errors:", oserrored, style="red")
    console.print("\n──────────────────────────────────────────────────────────\n")
    console.print("Metadata collected. Start replication? (Ctrl+C to cancel)", style="italic")
    input()

    # //──────────────────────────────────────────────────────────

    processed: deque[str] = deque()

    _default_columns = Progress.get_default_columns()
    progress = Progress(_default_columns[0], BarColumn(None), MofNCompleteColumn(), _default_columns[-1], expand=True)
    progress_files = progress.add_task("  Files:", total=total_files)
    progress_dirs = progress.add_task("Folders:", total=total_dirs)
    progress_links = progress.add_task("  Links:", total=total_links)
    progress.advance(progress_dirs, advance=-1)

    # with Progress(
    #     _default_columns[0],
    #     BarColumn(None),
    #     MofNCompleteColumn(),
    #     _default_columns[-1],
    #     expand=True,
    # ) as progress:
    #     progress_files = progress.add_task("  Files:", total=total_files)
    #     progress_dirs = progress.add_task("Folders:", total=total_dirs)
    #     progress_links = progress.add_task("  Links:", total=total_links)
    #     progress.advance(progress_dirs, advance=-1)

    progress0 = Progress(_default_columns[0], BarColumn(None), TransferSpeedColumn(), DownloadColumn(), _default_columns[-1], expand=True)
    progress_size = progress0.add_task("   Size:", total=total_size)

    progress_table = Table.grid()
    progress_table.add_row(progress0)
    progress_table.add_row(progress)

    with Live(progress_table, refresh_per_second=10):

        def ignore_controller(path: str, files: Sequence[str]) -> set[str]:
            """
            Ignore controller

            Argument `path` is a root to current working directory
            and `files` is a list of file names (without directory path) in that folder.
            Return would be an iterable of files to ignore.
            """

            nonlocal exclude, progress, progress_dirs  # , progress_files, list_files, list_dirs, list_links

            _exclude: deque[str] = deque()
            p = Path(path)

            for e in exclude:
                pe = Path(e)
                last_part = pe.parts[-1]

                # ? Strict check (directory path is equal to excluded directory)
                # Example: path: D:\test, exc: D:\test or path: D:\test\sub, exc: D:\test
                if p.is_relative_to(e):
                    _exclude.extend(files)

                # ? Pattern matching (with *)
                # Example: path: D:\test with some files and exc: D:\test\*.txt
                elif p.match(e):
                    _exclude.extend(fnmatch.filter(files, last_part))  # TODO: replace for faster alternative (fnmatch.translate is slow)

                # ? Files matching
                # Example: exc: D:\test\1.txt, path: D:\test, files ["1.txt", "2.txt"]
                elif pe.parent == p and (_files := fnmatch.filter(files, last_part)):  # TODO: same
                    _exclude.extend(_files)

            # progress.console.print(f"\nProcessing path: {path!r} Files:", files, "Path excluded:", path in exclude, _exclude)
            # input()
            to_be_processed = [(p / f) for f in files if f not in _exclude]
            processed.extend(map(str, to_be_processed))
            progress.advance(progress_dirs, advance=1)
            # NOTE: moved to copy2 function
            # progress.advance(progress_files, advance=len([f for f in to_be_processed if f.is_file()]))

            return set(_exclude)

        def copy_controller(source: str, destination: str, *, follow_symlinks: bool = True) -> str:
            """
            Copy controller

            For every file this function would be called with absolute paths for `source` and `destination`.
            Folders does not created with this function.
            """

            nonlocal progress, progress0, progress_files, progress_size, sizes

            progress.advance(progress_files, advance=1)
            progress0.advance(progress_size, advance=sizes[source])
            # progress.console.print(f'Copying: "{escape(source)}"') # → "{destination}"

            return copy2(source, destination, follow_symlinks=follow_symlinks)

        try:
            copytree(source, destination, symlinks=True, ignore=ignore_controller, copy_function=copy_controller, ignore_dangling_symlinks=True, dirs_exist_ok=dir_exist_ok)
        except FileExistsError as e:
            inspect(e)
        except ShutilError as e:
            error_files = {n: er for n, _, er in e.args[0]}

            # inspect(tuple(error_files.items()))
            progress.advance(progress_files, advance=-len(error_files))
            progress.console.print("Errors during file or directory copy:", style="red")

            for f, er in error_files.items():
                progress.console.print(f"{f}:", er, style="red")
                with contextlib.suppress(ValueError):
                    processed.remove(f)

            progress.console.print()
        except KeyboardInterrupt:
            progress.console.print("Cancelling task. You can restart program and continue from latest file.", style="blue")

            with progress_file.open(mode="a", encoding="utf-8") as f:
                f.writelines(p + "\n" for p in islice(processed, len(processed) - 1))

            raise

        # //──────────────────────────────────────────────────────────

        if IS_WINDOWS and (extra_attrib_dirs or junction_dirs):
            kernel32 = ctypes.WinDLL("kernel32.dll", use_last_error=True)

            for folder in extra_attrib_dirs:
                attrs = folder.stat(follow_symlinks=False).st_file_attributes
                target = Path(destination, *folder.parts[len_source:])

                if kernel32.SetFileAttributesW(str(target), attrs):
                    progress.console.print(f"Copy stat from {folder!r} to {target!r}: {attrs}")
                    # progress.advance(progress_links, advance=1)
                else:
                    oserrored.append(folder)
                    progress.console.print(f"Copy stat error at {target!r}", style="red")

                # if (attrs := kernel32.GetFileAttributesW(str(target))) == INVALID_FILE_ATTRIBUTES:
                #     raise ctypes.WinError(ctypes.get_last_error())
                #     continue

                # attrs |= stat.FILE_ATTRIBUTE_HIDDEN
                # if not kernel32.SetFileAttributesW(target, attrs):
                #     raise ctypes.WinError(ctypes.get_last_error())

            progress.console.print()

            for folder in junction_dirs:
                attrs = folder.stat(follow_symlinks=False).st_file_attributes
                folder_source = folder.readlink()
                target_source = Path(destination, *folder_source.parts[len_source:])
                target = Path(destination, *folder.parts[len_source:])
                # target = Path(f"{os.sep * 2}?{os.sep}{destination.parts[0]}", *destination.parts[1:], *folder_source.parts[len_source:])

                try:
                    create_junction_point(target_source, target)
                    copystat(folder, target)
                    kernel32.SetFileAttributesW(str(target), attrs)

                    progress.console.print(f"Created Junction point from {target_source!r} to {target!r}")
                    progress.advance(progress_links, advance=1)
                except OSError as e:
                    oserrored.append(folder)
                    progress.console.print("Junction creation error:", e, f"at {target!r}", style="red")

            for _hl_source, hl_list in hardlinks.items():
                for _hl in hl_list:
                    if _hl == _hl_source:
                        continue

                    try:
                        hl_source = Path(destination, *_hl_source.parts[len_source:])
                        hl = Path(destination, *_hl.parts[len_source:])
                        hl.hardlink_to(hl_source)

                        attrs = _hl.stat(follow_symlinks=False).st_file_attributes
                        kernel32.SetFileAttributesW(str(hl), attrs)

                        progress.advance(progress_links, advance=1)
                        progress.console.print(f"Created hardlink from {hl_source!r} to {hl!r}")
                    except FileExistsError as e:
                        progress.console.print("Hardlink creation error:", e)

            progress.console.print()

        progress.advance(progress_dirs, advance=1)

    with progress_file.open(mode="a", encoding="utf-8") as f:
        f.writelines(p + "\n" for p in processed)

    if oserrored:
        with progress_file.with_suffix(".error.json").open(mode="ab") as f:
            f.write(orjson.dumps([str(e) for e in oserrored]))

    # console.print("\nProcessed:")
    # console.print(processed)
    console.print("Replication completed", style="bold green")

    return not oserrored


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

    raise NotImplementedError


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
