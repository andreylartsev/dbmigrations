"""
Simple database migrations tool
"""

import argparse

class UpdateCommand:
    """Apply changes on the database schema"""
    def __init__(self, subparsers):
        parser = subparsers.add_parser("update", help=UpdateCommand.__doc__)
        parser.add_argument("-f","--force", action="store_true", default=False, help="Force apply updates even if version control tables does not exists")
        parser.set_defaults(exec=self) 
    def __call__(self, args):
        print(f"Apply updates force={args.force}")

class VerifyCommand:
    """Verify which of versioned and repeatable scripts will be applied on specified database schema"""
    def __init__(self, subparsers):
        parser = subparsers.add_parser("verify", help=VerifyCommand.__doc__)
        parser.set_defaults(exec=self) 
    def __call__(self, args):
        print(f"Verify database schema")

class CleanupCommand:
    """Cleanup specified database schema"""
    def __init__(self, subparsers):
        parser = subparsers.add_parser("cleanup", help=CleanupCommand.__doc__)
        parser.add_argument("-f","--force", action="store_true", default=False, help="Force clean even if version control tables does not exists")
        parser.set_defaults(exec=self) 
    def __call__(self, args):
        print(f"Cleanup database schema {args.force}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)    
    subparsers = parser.add_subparsers(dest="cmd", help="Available subcommands")

    UpdateCommand(subparsers)

    VerifyCommand(subparsers)

    CleanupCommand(subparsers)

    # Parse arguments
    args = parser.parse_args()

    # Call the function associated with the subcommand
    if hasattr(args, 'exec'):
        args.exec(args)
    else:
        # If no subcommand is given, print help (or handle as needed)
        parser.print_help()

if __name__ == "__main__":
    main()
