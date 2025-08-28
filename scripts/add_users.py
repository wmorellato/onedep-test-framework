import os
import django

os.environ["DJANGO_SETTINGS_MODULE"] = "wwpdb.apps.deposit.settings"
django.setup()

import click
import requests

from wwpdb.apps.deposit.auth.tokens import create_token

MAIN_ORCID = "0000-0002-5109-8728"  # Replace with your main ORCID if needed
API_BASE_URL = "https://wp-p3s-87.ebi.ac.uk:12000/deposition/api/v1/depositions"


@click.command()
@click.option("--dep-ids", required=True, help="Comma-separated deposition IDs")
@click.option("--orcids", required=True, help="Comma-separated ORCIDs")
def add_users(dep_ids, orcids):
    """
    Add one or more ORCIDs to one or more depositions.
    """
    dep_ids = [d.strip() for d in dep_ids.split(",") if d.strip()]
    orcids = [o.strip() for o in orcids.split(",") if o.strip()]
    token = create_token(MAIN_ORCID, expiration_days=7)

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    payload = [{"orcid": orcid} for orcid in orcids]

    for dep_id in dep_ids:
        url = f"{API_BASE_URL}/{dep_id}/users/"
        click.echo(f"➡️ Sending request to {url} with ORCIDs: {orcids}")

        response = requests.post(url, json=payload, headers=headers, verify=False)

        if response.status_code in (200, 201):
            click.echo(f"✅ Successfully added users to deposition {dep_id}")
        else:
            click.echo(f"❌ Failed for deposition {dep_id}: {response.status_code} {response.text}")

if __name__ == "__main__":
    add_users()
