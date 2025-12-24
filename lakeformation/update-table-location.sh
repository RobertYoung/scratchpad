#!/bin/bash
# Updates a Glue table's location from S3 Access Point to S3 bucket path
# This is useful when a crawler created a table with an Access Point location
# but Athena needs a regular S3 bucket path to query the data.
#
# Usage: ./update-table-location.sh <database> <table> <new-s3-location>
# Example: ./update-table-location.sh my_database my_table s3://my-bucket/my-prefix/

set -e

DATABASE_NAME="$1"
TABLE_NAME="$2"
NEW_S3_LOCATION="$3"

# Validate arguments
if [[ -z "$DATABASE_NAME" || -z "$TABLE_NAME" || -z "$NEW_S3_LOCATION" ]]; then
    echo "Usage: $0 <database> <table> <new-s3-location>"
    echo "Example: $0 my_database my_table s3://my-bucket/my-prefix/"
    exit 1
fi

# Validate S3 location format
if [[ ! "$NEW_S3_LOCATION" =~ ^s3:// ]]; then
    echo "Error: new-s3-location must start with s3://"
    exit 1
fi

echo "Fetching current table definition..."
TABLE_DEF=$(aws glue get-table --database-name "$DATABASE_NAME" --name "$TABLE_NAME")

# Show current location
CURRENT_LOCATION=$(echo "$TABLE_DEF" | jq -r '.Table.StorageDescriptor.Location')
echo "Current location: $CURRENT_LOCATION"
echo "New location: $NEW_S3_LOCATION"

# Build the table input for update (remove fields that can't be updated)
TABLE_INPUT=$(echo "$TABLE_DEF" | jq --arg loc "$NEW_S3_LOCATION" '
    .Table |
    del(.DatabaseName, .CreateTime, .UpdateTime, .CreatedBy, .IsRegisteredWithLakeFormation, .CatalogId, .VersionId, .IsMultiDialectView) |
    .StorageDescriptor.Location = $loc
')

echo "Updating table location..."
aws glue update-table \
    --database-name "$DATABASE_NAME" \
    --table-input "$TABLE_INPUT"

echo "Verifying update..."
UPDATED_LOCATION=$(aws glue get-table --database-name "$DATABASE_NAME" --name "$TABLE_NAME" | jq -r '.Table.StorageDescriptor.Location')
echo "Updated location: $UPDATED_LOCATION"

if [[ "$UPDATED_LOCATION" == "$NEW_S3_LOCATION" ]]; then
    echo "Success! Table location updated."
else
    echo "Warning: Location may not have been updated correctly."
    exit 1
fi
