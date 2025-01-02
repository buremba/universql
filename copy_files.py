import os
import shutil
from pathlib import Path
from typing import Optional
import re
from datetime import datetime
import subprocess
import sys

# CONFIGURATION
GITIGNORE_FILE = Path('/Users/ryanwaldorf/dev/universql/universql/.gitignore') # optional, set to None if not needed
SOURCE_DIRECTORY = Path('/Users/ryanwaldorf/dev/universql/universql')  # Set your source directory
TARGET_DIRECTORY = Path('/Users/ryanwaldorf/dev/universql/universql_files_for_claude')  # Set your target directory

# File extensions to copy, optional, set to None if not needed
EXTENSIONS = {'.py', '.yaml', '.toml', '.txt', '.md', '.gitignore', '.gitattributes'}

def is_system_directory(path: Path) -> bool:
    """Check if path is a protected system directory"""
    system_dirs = {
        # Unix/Linux/Mac
        '/bin', '/sbin', '/usr', '/etc', '/var', '/tmp', '/root', '/sys', 
        '/proc', '/dev', '/opt', '/lib', '/lib64', '/boot', '/home',
        # Windows
        'C:\\Windows', 'C:\\Program Files', 'C:\\Program Files (x86)', 
        'C:\\ProgramData', 'C:\\System32', 'C:\\Users'
    }
    
    try:
        resolved_path = path.resolve()
        str_path = str(resolved_path).replace('\\', '/')
        
        # Check if path is or contains a system directory
        for sys_dir in system_dirs:
            sys_dir = sys_dir.replace('\\', '/')
            if str_path.startswith(sys_dir) or sys_dir.startswith(str_path):
                return True
                
        # Check if path is root
        if len(resolved_path.parts) <= 1:
            return True
            
        # Additional Unix checks
        if os.name == 'posix':
            stat = os.stat(resolved_path)
            # Check if owned by root or system
            if stat.st_uid == 0:
                return True
                
        return False
        
    except (OSError, PermissionError):
        # If we can't check, assume it's protected
        return True

def validate_directory(path: Path, is_target: bool = False) -> bool:
    """Validate directory is safe to use"""
    try:
        path = Path(path).resolve()
        
        if is_system_directory(path):
            print(f"Error: Cannot use system directory: {path}")
            return False
            
        if is_target:
            # Additional checks for target directory
            if path.exists():
                # Check write permission
                if not os.access(path, os.W_OK):
                    print(f"Error: No write permission for target directory: {path}")
                    return False
            else:
                # Check parent write permission
                parent = path.parent
                if not os.access(parent, os.W_OK):
                    print(f"Error: No write permission for parent directory: {parent}")
                    return False
                    
        return True
        
    except (OSError, PermissionError) as e:
        print(f"Error accessing directory {path}: {e}")
        return False

def run_git_command(directory: Path, command: list[str]) -> tuple[bool, str]:
    """
    Run a git command in the specified directory
    Returns (success, output) tuple
    """
    try:
        result = subprocess.run(
            ['git'] + command,
            cwd=directory,
            capture_output=True,
            text=True
        )
        return result.returncode == 0, result.stdout.strip()
    except Exception as e:
        return False, str(e)

def check_parent_git_repository(directory: Path) -> bool:
    """
    Check if any parent directory contains a .git folder
    Creates directory if it doesn't exist
    Returns True if a parent git repository is found
    """
    directory = directory.resolve()
    if not directory.exists():
        directory.mkdir(parents=True)
        print(f"Created directory: {directory}")
        return False
        
    current = directory
    while current != current.parent:
        if (current / '.git').is_dir():
            if current != directory:
                print(f"Found existing git repository in parent directory: {current}")
                return True
        current = current.parent
    return False

def init_git_repository(directory: Path) -> bool:
    """
    Initialize a new git repository if one doesn't exist
    Creates directory if it doesn't exist
    Returns True if successful
    """
    directory = directory.resolve()
    if not directory.exists():
        directory.mkdir(parents=True)
        print(f"Created directory: {directory}")
        
    if not (directory / '.git').is_dir():
        success, output = run_git_command(directory, ['init'])
        if success:
            print(f"Initialized new git repository in: {directory}")
            return True
        else:
            print(f"Failed to initialize git repository: {output}")
            return False
    return True

def commit_changes(directory: Path, message: str) -> bool:
    """
    Add all files and create a commit
    Returns True if successful
    """
    success, _ = run_git_command(directory, ['add', '.'])
    if not success:
        print("Failed to add files to git")
        return False
    
    success, _ = run_git_command(directory, ['commit', '-m', message])
    if not success:
        print("Failed to create commit")
        return False
    
    print(f"Successfully committed changes with message: {message}")
    return True

class GitignoreParser:
    def __init__(self, gitignore_file: Optional[Path] = None):
        self.ignore_patterns = []
        
        if gitignore_file is None:
            gitignore_file = GITIGNORE_FILE
        
        if gitignore_file and gitignore_file.exists():
            with open(gitignore_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        pattern = line.replace('.', r'\.')
                        pattern = pattern.replace('*', '.*')
                        pattern = pattern.replace('?', '.')
                        if line.endswith('/'):
                            pattern = f"(^|.*?/){pattern}.*$"
                        elif line.startswith('/'):
                            pattern = f"^{pattern[1:]}.*$"
                        elif '/' in line:
                            pattern = f"^{pattern}.*$"
                        else:
                            pattern = f"(^|.*?/){pattern}($|/.*$)"
                        self.ignore_patterns.append(re.compile(pattern))
            print(f"Loaded gitignore patterns from: {gitignore_file}")
        else:
            print("No gitignore patterns loaded")

    def should_ignore(self, path: str) -> bool:
        if not self.ignore_patterns:
            return False
            
        path_str = str(path)
        for pattern in self.ignore_patterns:
            if pattern.match(path_str):
                return True
        return False

def clean_directory(directory: Path, exclude_git: bool = True):
    """
    Clean directory while optionally preserving .git folder
    Creates directory if it doesn't exist
    """
    dir_path = Path(directory).resolve()
    
    # Validate directory before cleaning
    if not validate_directory(dir_path, is_target=True):
        print("Error: Cannot clean invalid or protected directory")
        return
        
    if not dir_path.exists():
        dir_path.mkdir(parents=True)
        print(f"Created directory: {dir_path}")
        return

    if exclude_git and (dir_path / '.git').exists():
        # Remove everything except .git
        for item in dir_path.iterdir():
            if item.name != '.git':
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
        print(f"Cleaned directory (preserved .git): {dir_path}")
    else:
        if dir_path.exists():
            shutil.rmtree(dir_path)
        dir_path.mkdir(parents=True)
        print(f"Cleaned and recreated directory: {dir_path}")

def create_unique_filename(target_dir: Path, original_name: str) -> str:
    name = Path(original_name).stem
    suffix = Path(original_name).suffix
    counter = 1
    new_name = original_name
    
    while (target_dir / new_name).exists():
        new_name = f"{name}_{counter}{suffix}"
        counter += 1
    
    return new_name

def copy_specific_files(source_dir: Path = SOURCE_DIRECTORY, 
                       target_dir: Path = TARGET_DIRECTORY):
    # Validate directories first
    if not validate_directory(source_dir):
        print("Error: Invalid source directory")
        return None
        
    if not validate_directory(target_dir, is_target=True):
        print("Error: Invalid target directory")
        return None
    
    source_dir = Path(source_dir).resolve()
    target_dir = Path(target_dir).resolve()
    
    # 1. Check for parent git repository
    if check_parent_git_repository(target_dir):
        print("Error: Target directory must not be within an existing git repository")
        return None
    
    # 2. Initialize git repository
    if not init_git_repository(target_dir):
        print("Error: Failed to initialize git repository")
        return None
    
    # 3. Clean directory (preserve .git)
    clean_directory(target_dir, exclude_git=True)
    
    gitignore = GitignoreParser(source_dir / '.gitignore')
    files_copied = 0
    files_ignored = 0
    file_mappings = []
    
    for root, dirs, files in os.walk(source_dir):
        rel_root = Path(root).relative_to(source_dir)
        
        if 'temp' in Path(root).parts:
            continue
            
        dirs[:] = [d for d in dirs if not d.startswith('_') and not gitignore.should_ignore(str(rel_root / d))]    
        
        for file in files:
            file_path = Path(root) / file
            rel_path = file_path.relative_to(source_dir)
            
            if gitignore.should_ignore(str(rel_path)):
                print(f"Ignoring (gitignore): {rel_path}")
                files_ignored += 1
                continue

            if file.startswith('_'):
                print(f"Ignoring (underscore): {rel_path}")
                files_ignored += 1
                continue
            
            if (file_path.suffix.lower() in EXTENSIONS or 
                file in {'.gitignore', '.gitattributes'}):
                
                new_filename = create_unique_filename(target_dir, file)
                target_path = target_dir / new_filename
                
                shutil.copy2(file_path, target_path)
                print(f"Copied: {rel_path} -> {new_filename}")
                files_copied += 1
                
                file_mappings.append({
                    'new_file': new_filename,
                    'original_path': str(rel_path),
                    'original_directory': str(rel_root)
                })

    # Create file mapping
    mapping_path = target_dir / 'file_map.txt'
    with open(mapping_path, 'w') as f:
        f.write(f"File Mapping Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Source Directory: {source_dir}\n")
        f.write("-" * 80 + "\n\n")
        
        file_mappings.sort(key=lambda x: x['original_path'])
        
        for mapping in file_mappings:
            f.write(f"New filename: {mapping['new_file']}\n")
            f.write(f"Original path: {mapping['original_path']}\n")
            f.write(f"Original directory: {mapping['original_directory']}\n")
            f.write("-" * 40 + "\n")

    # 4. Commit changes
    if not commit_changes(target_dir, "Initial commit with copied files"):
        print("Warning: Failed to commit changes")
        return None

    return files_copied, files_ignored

if __name__ == '__main__':
    try:
        result = copy_specific_files()
        if result is not None:
            files_copied, files_ignored = result
            print(f"\nFile copying completed successfully!")
            print(f"Total files copied: {files_copied}")
            print(f"Files ignored due to gitignore rules: {files_ignored}")
            print(f"File mapping has been created in file_map.txt")
        else:
            sys.exit(1)
    except Exception as e:
        print(f"\nAn error occurred: {str(e)}")
        sys.exit(1)