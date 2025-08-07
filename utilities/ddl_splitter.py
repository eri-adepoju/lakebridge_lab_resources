#!/usr/bin/env python3
"""
DDL Splitter - Splits large DDL files into individual object files.

This script efficiently processes large DDL files (e.g., from Snowflake GET_DDL)
and splits them into separate files for each database object.

Supports both single files and entire folders containing multiple DDL files.

Usage Examples:
  python3 ddl_splitter.py database_ddl.sql
  python3 ddl_splitter.py database_ddl.sql ./output_folder
  python3 ddl_splitter.py ./ddl_folder
  python3 ddl_splitter.py ./ddl_folder ./output_folder
  python3 ddl_splitter.py --help
"""

import os
import re
import sys
from pathlib import Path
from typing import Generator, Tuple, List


class DDLSplitter:
    """Efficiently splits DDL files into individual object files."""
    
    # Common DDL statement patterns
    DDL_PATTERNS = [
        r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMP(?:ORARY)?\s+)?TABLE\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:MATERIALIZED\s+)?VIEW\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?SEQUENCE\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?STREAM\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?TASK\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?PIPE\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?FILE\s+FORMAT\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?WAREHOUSE\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?DATABASE\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?SCHEMA\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:MASKING|ROW\s+ACCESS|AGGREGATION|AUTHENTICATION|JOIN|PASSWORD|PRIVACY|PROJECTION|SESSION)\s+POLICY\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?TAG\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?STORAGE\s+INTEGRATION\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:EXTERNAL|HYBRID|ICEBERG)\s+TABLE\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?DYNAMIC\s+TABLE\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?EVENT\s+TABLE\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?SEMANTIC\s+VIEW\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?ALERT\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?DBT\s+PROJECT\s+[^;]+;',
        r'CREATE\s+(?:OR\s+REPLACE\s+)?DATA\s+METRIC\s+FUNCTION\s+[^;]+;',
    ]
    
    def __init__(self, input_path: str, output_dir: str = None):
        """Initialize the DDL splitter.
        
        Args:
            input_path: Path to the input DDL file or folder
            output_dir: Directory to save split files (defaults to input_path directory)
        """
        self.input_path = Path(input_path)
        self.output_dir = Path(output_dir) if output_dir else self._get_default_output_dir()
        
    def _get_default_output_dir(self) -> Path:
        """Get default output directory based on input type."""
        if self.input_path.is_file():
            return self.input_path.parent / f"{self.input_path.stem}_split"
        else:
            return self.input_path.parent / f"{self.input_path.name}_split"
        
    def _find_ddl_files(self, directory: Path) -> List[Path]:
        """Find all DDL files in a directory recursively."""
        ddl_files = []
        sql_extensions = ['.sql', '.ddl', '.SQL', '.DDL']
        
        for file_path in directory.rglob('*'):
            if file_path.is_file() and file_path.suffix in sql_extensions:
                ddl_files.append(file_path)
        
        return sorted(ddl_files)
    
    def _read_file_chunks(self, chunk_size: int = 8192) -> Generator[str, None, None]:
        """Read file in chunks for memory efficiency."""
        with open(self.input_path, 'r', encoding='utf-8') as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk
    
    def _extract_object_name(self, ddl_statement: str) -> str:
        """Extract object name from DDL statement."""
        # Remove CREATE OR REPLACE and get the object type and name
        clean_ddl = re.sub(r'CREATE\s+(?:OR\s+REPLACE\s+)?', '', ddl_statement, flags=re.IGNORECASE)
        
        # Extract object type and name
        match = re.match(r'(\w+)\s+([^\s(]+)', clean_ddl.strip(), re.IGNORECASE)
        if match:
            object_type, object_name = match.groups()
            # Clean up object name (remove quotes, schema prefixes)
            clean_name = re.sub(r'["\']', '', object_name)
            clean_name = clean_name.split('.')[-1]  # Remove schema prefix
            return f"{object_type.lower()}_{clean_name}"
        
        return f"object_{hash(ddl_statement) % 10000}"
    
    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for filesystem compatibility."""
        # Replace invalid characters
        sanitized = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # Limit length
        if len(sanitized) > 200:
            sanitized = sanitized[:200]
        return sanitized
    
    def split_single_file(self, input_file: Path, output_dir: Path) -> Tuple[int, int]:
        """Split a single DDL file into individual object files.
        
        Args:
            input_file: Path to the input DDL file
            output_dir: Directory to save split files
            
        Returns:
            Tuple of (total_statements, successful_splits)
        """
        print(f"🔍 Processing: {input_file}")
        print(f"📁 Output directory: {output_dir}")
        print()
        
        # Create output directory if it doesn't exist
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Read entire file content
        with open(input_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Remove comments and normalize whitespace
        content = re.sub(r'--.*$', '', content, flags=re.MULTILINE)
        content = re.sub(r'/\*.*?\*/', '', content, flags=re.DOTALL)
        content = re.sub(r'\s+', ' ', content)
        
        # Find all DDL statements
        all_statements = []
        for pattern in self.DDL_PATTERNS:
            statements = re.findall(pattern, content, re.IGNORECASE | re.DOTALL)
            all_statements.extend(statements)
        
        if not all_statements:
            print("⚠️  No DDL statements found with standard patterns. Trying alternative parsing...")
            # Fallback: split by semicolon and look for CREATE statements
            statements = content.split(';')
            all_statements = [stmt.strip() + ';' for stmt in statements 
                            if stmt.strip().upper().startswith('CREATE')]
        
        print(f"📊 Found {len(all_statements)} DDL statements")
        print()
        
        successful_splits = 0
        for i, statement in enumerate(all_statements, 1):
            if not statement.strip():
                continue
                
            try:
                # Extract object name
                object_name = self._extract_object_name(statement)
                sanitized_name = self._sanitize_filename(object_name)
                
                # Create filename with source file prefix
                source_prefix = input_file.stem
                filename = f"{source_prefix}_{i:04d}_{sanitized_name}.sql"
                filepath = output_dir / filename
                
                # Write statement to file
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(statement.strip() + '\n')
                
                successful_splits += 1
                print(f"  ✅ {filename}")
                
            except Exception as e:
                print(f"  ❌ Error processing statement {i}: {e}")
        
        print()
        print(f"🎉 Split complete: {successful_splits}/{len(all_statements)} statements processed")
        return len(all_statements), successful_splits
    
    def split_ddl(self) -> Tuple[int, int]:
        """Split DDL file(s) into individual object files.
        
        Returns:
            Tuple of (total_files_processed, total_statements_processed)
        """
        if self.input_path.is_file():
            # Single file mode
            total_statements, successful_splits = self.split_single_file(self.input_path, self.output_dir)
            return 1, successful_splits
        else:
            # Folder mode
            ddl_files = self._find_ddl_files(self.input_path)
            
            if not ddl_files:
                print(f"❌ No DDL files found in: {self.input_path}")
                print("💡 Looking for files with extensions: .sql, .ddl, .SQL, .DDL")
                return 0, 0
            
            print(f"📂 Found {len(ddl_files)} DDL files in: {self.input_path}")
            print(f"📁 Output directory: {self.output_dir}")
            print()
            
            total_files_processed = 0
            total_statements_processed = 0
            
            for i, ddl_file in enumerate(ddl_files, 1):
                print(f"📄 Processing file {i}/{len(ddl_files)}: {ddl_file.name}")
                print("-" * 60)
                
                # Create subdirectory for each file
                file_output_dir = self.output_dir / ddl_file.stem
                
                try:
                    statements, successful = self.split_single_file(ddl_file, file_output_dir)
                    total_statements_processed += successful
                    total_files_processed += 1
                except Exception as e:
                    print(f"❌ Error processing {ddl_file.name}: {e}")
                
                print()
            
            print(f"🎉 Batch processing complete!")
            print(f"📊 Files processed: {total_files_processed}/{len(ddl_files)}")
            print(f"📊 Total statements processed: {total_statements_processed}")
            
            return total_files_processed, total_statements_processed


def print_help():
    """Print helpful usage information."""
    print("""
🔧 DDL Splitter - Split large DDL files into individual object files

USAGE:
  python3 ddl_splitter.py <input_file_or_folder> [output_directory]
  python3 ddl_splitter.py --help
  python3 ddl_splitter.py -h

EXAMPLES:
  # Single file
  python3 ddl_splitter.py database_ddl.sql
  python3 ddl_splitter.py database_ddl.sql ./split_output
  
  # Folder with multiple DDL files
  python3 ddl_splitter.py ./ddl_folder
  python3 ddl_splitter.py ./ddl_folder ./batch_output
  python3 ddl_splitter.py /path/to/ddl_files /path/to/output

WHAT IT DOES:
  • Takes a large DDL file OR a folder containing multiple DDL files
  • Splits each file into individual files for each database object
  • Creates numbered files like: 0001_table_customers.sql, 0002_view_orders.sql
  • For folders: creates subdirectories for each source file
  • Supports all major Snowflake object types

SUPPORTED OBJECTS:
  • Tables, Views, Functions, Procedures
  • Sequences, Streams, Tasks, Pipes
  • Warehouses, Databases, Schemas
  • Policies, Tags, Storage Integrations
  • And many more...

SUPPORTED FILE TYPES:
  • .sql, .ddl, .SQL, .DDL

FOLDER MODE:
  • Processes all DDL files in the folder and subfolders
  • Creates separate output subdirectories for each source file
  • Example: input_folder/file1.sql → output_folder/file1/0001_table_*.sql

TIPS:
  • If no output directory is specified, creates a folder named '<input>_split'
  • Files are numbered for easy ordering
  • Object names are automatically extracted from DDL statements
  • Folder mode is great for processing entire database exports
""")


def get_user_input():
    """Get input file or folder from user interactively."""
    while True:
        try:
            input_path = input("📄 Enter the path to your DDL file or folder: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n👋 Goodbye!")
            sys.exit(0)
            
        if not input_path:
            print("❌ Please enter a file or folder path.")
            continue
        
        if not os.path.exists(input_path):
            print(f"❌ Path not found: {input_path}")
            continue
        
        # Check if it's a file with SQL extension
        if os.path.isfile(input_path):
            if not input_path.lower().endswith(('.sql', '.ddl')):
                try:
                    print("⚠️  Warning: File doesn't have .sql or .ddl extension. Continue anyway? (y/n): ", end='')
                    response = input().lower()
                    if response not in ['y', 'yes']:
                        continue
                except (EOFError, KeyboardInterrupt):
                    print("\n👋 Goodbye!")
                    sys.exit(0)
        
        return input_path


def get_output_directory():
    """Get output directory from user interactively."""
    try:
        output_dir = input("📁 Enter output directory (press Enter for default): ").strip()
        if not output_dir:
            return None
        return output_dir
    except (EOFError, KeyboardInterrupt):
        print("\n👋 Goodbye!")
        sys.exit(0)


def main():
    """Main function to handle command line usage."""
    # Check for help flags
    if len(sys.argv) > 1 and sys.argv[1] in ['--help', '-h', 'help']:
        print_help()
        return
    
    # Interactive mode if no arguments provided
    if len(sys.argv) == 1:
        print("🔧 DDL Splitter - Interactive Mode")
        print("=" * 40)
        print()
        
        try:
            input_path = get_user_input()
            output_dir = get_output_directory()
            
            print()
            print("🚀 Starting DDL split process...")
            print()
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            return
    
    # Command line mode
    elif len(sys.argv) < 2:
        print("❌ Error: Input file or folder is required.")
        print()
        print("Quick help:")
        print("  python3 ddl_splitter.py <input_file_or_folder> [output_directory]")
        print("  python3 ddl_splitter.py --help")
        sys.exit(1)
    
    else:
        input_path = sys.argv[1]
        output_dir = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Validate input path
    if not os.path.exists(input_path):
        print(f"❌ Error: Input path '{input_path}' not found.")
        print("💡 Make sure the file or folder path is correct and exists.")
        sys.exit(1)
    
    try:
        splitter = DDLSplitter(input_path, output_dir)
        files_processed, statements_processed = splitter.split_ddl()
        
        print()
        if statements_processed > 0:
            if files_processed > 1:
                print(f"🎉 Successfully processed {files_processed} files with {statements_processed} DDL statements!")
            else:
                print(f"🎉 Successfully split {statements_processed} DDL statements!")
            print(f"📁 Files saved to: {splitter.output_dir}")
            print()
            print("💡 Tip: You can now run individual DDL statements or review them separately.")
        else:
            print("❌ No DDL statements were successfully processed.")
            print("💡 Check if your files contain valid DDL statements starting with 'CREATE'.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n⏹️  Process interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        print("💡 Make sure the files are valid SQL/DDL files and you have write permissions.")
        sys.exit(1)


if __name__ == "__main__":
    main() 