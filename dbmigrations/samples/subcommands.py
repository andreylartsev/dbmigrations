import argparse

import argparse

def commit(args):
    """Function to handle the 'commit' subcommand."""
    print(f"Committing with message: '{args.message}' and all_files={args.all_files}")
    # Add your commit logic here

def push(args):
    """Function to handle the 'push' subcommand."""
    print(f"Pushing to remote: '{args.remote}'")
    # Add your push logic here

def main():
    parser = argparse.ArgumentParser(description="A simple version control system CLI")
    
    # Add subparsers with a destination variable 'command'
    subparsers = parser.add_subparsers(dest="command", help="Available subcommands")
    
    # Create the parser for the "commit" command
    commit_parser = subparsers.add_parser("commit", help="Record changes to the repository")
    commit_parser.add_argument("-m", "--message", type=str, required=True, help="A commit message")
    commit_parser.add_argument("--all-files", action="store_true", help="Commit all modified files")
    commit_parser.set_defaults(func=commit) # Set the function to call

    # Create the parser for the "push" command
    push_parser = subparsers.add_parser("push", help="Push changes to a remote repository")
    push_parser.add_argument("remote", type=str, default="origin", nargs="?", help="The remote name (default: origin)")
    push_parser.set_defaults(func=push) # Set the function to call

    # Parse arguments
    args = parser.parse_args()

    # Call the function associated with the subcommand
    if hasattr(args, 'func'):
        args.func(args)
    else:
        # If no subcommand is given, print help (or handle as needed)
        parser.print_help()

if __name__ == "__main__":
    main()
