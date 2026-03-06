"""Email authentication utility for Outlook integration.

Handles the one-time OAuth 2.0 setup to obtain a refresh token for
accessing Microsoft Graph API.

Implements the authentication requirements from specs/core/EMAIL_INTEGRATION.md.
"""

import argparse
import os
import sys
from O365 import Account


def run_setup() -> None:
    """Run the interactive OAuth 2.0 setup workflow."""
    print("=" * 60)
    print("newshive EMAIL AUTHENTICATION SETUP")
    print("=" * 60)
    print("\nThis utility will help you obtain a refresh token for Outlook access.")
    print("Ensure you have created an App Registration in Azure Portal first.")

    client_id = os.getenv("AZURE_CLIENT_ID") or os.getenv("OUTLOOK_CLIENT_ID")
    client_secret = os.getenv("AZURE_CLIENT_SECRET") or os.getenv("OUTLOOK_CLIENT_SECRET")
    tenant_id = os.getenv("AZURE_TENANT_ID") or os.getenv("OUTLOOK_TENANT_ID")

    if not all([client_id, client_secret, tenant_id]):
        print("\nError: Missing required environment variables.")
        print("Please set the following and try again:")
        print("- AZURE_CLIENT_ID")
        print("- AZURE_CLIENT_SECRET")
        print("- AZURE_TENANT_ID")
        sys.exit(1)

    credentials = (client_id, client_secret)
    # Mail.Read is enough for discovery
    scopes = ["Mail.Read", "offline_access"]

    account = Account(credentials, tenant_id=tenant_id)
    if account.authenticate(scopes=scopes):
        print("\nAuthentication successful!")
        # The token is saved by default in o365_token.txt or similar depending on library version
        # But we really want the refresh token to be used as an environment variable
        # for more flexible deployments.

        token = account.connection.token_backend.load_token()
        if token and "refresh_token" in token:
            refresh_token = token["refresh_token"]
            print("\n" + "=" * 60)
            print("YOUR REFRESH TOKEN")
            print("=" * 60)
            print(f"\n{refresh_token}")
            print("\n" + "=" * 60)
            print("\nIMPORTANT: Store this token in the AZURE_REFRESH_TOKEN environment variable.")
            print("Keep it secret! Never commit it to version control.")
        else:
            print("\nError: Authentication succeeded but no refresh token was found.")
            print("Ensure 'offline_access' scope was granted.")
    else:
        print("\nAuthentication failed. Please check your credentials and try again.")


def main() -> None:
    parser = argparse.ArgumentParser(description="newshive Email Authentication Utility")
    parser.add_argument("--setup", action="store_true", help="Run the OAuth 2.0 setup workflow")

    args = parser.parse_args()

    if args.setup:
        run_setup()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
