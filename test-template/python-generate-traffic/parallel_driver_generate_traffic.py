#!/usr/bin/env -S python3 -u

# This file serves as a parallel driver (https://antithesis.com/docs/test_templates/test_composer_reference/#parallel-driver). 
# It does between 1 and 100 random kv puts against a random etcd host in the cluster. We then check to see if successful puts persisted
# and are correct on another random etcd host.

# Antithesis SDK
from antithesis.assertions import (
    always,
    sometimes,
)

import sys
import os
sys.path.append("/opt/antithesis/resources")
import helper


def simulate_traffic():
    """
        This function will first connect to an etcd host, then execute a certain number of put requests. 
        The key and value for each put request are generated using Antithesis randomness (check within the helper.py file). 
        We return the key/value pairs from successful requests.
    """
    client = helper.connect_to_host()
    num_requests = helper.generate_requests()
    kvs = []
    mismatched_once = False

    for _ in range(num_requests):

        # generating random str for the key and value
        key = helper.generate_random_string()
        value = helper.generate_random_string()

        # response of the put request
        success, error = helper.put_request(client, key, value)

        # Antithesis Assertion: sometimes put requests are successful. A failed request is OK since we expect them to happen.
        sometimes(success, "Client can make successful put requests", {"error":error})

        if success:
            # Track whether this write was intentionally modified (for demo / validation only)
            intentionally_modified = False

            # Optional (OFF by default): introduce exactly one mismatch to demonstrate
            # that the per-key `always(...)` assertion is active and observable
            if os.getenv("INTENTIONAL_MISMATCH") == "1" and not mismatched_once:
                value = value + "_INTENTIONAL_MISMATCH"
                mismatched_once = True
                intentionally_modified = True

            kvs.append((key, value))

            # Make the intentional mismatch explicit in logs to avoid confusion
            if intentionally_modified:
                print(
                    f"Client: successful put with key '{key}' and value '{value}' "
                    "(INTENTIONAL_MISMATCH applied for assertion validation)"
                )
            else:
                print(f"Client: successful put with key '{key}' and value '{value}'")
        else:
            print(f"Client: unsuccessful put with key '{key}', value '{value}', and error '{error}'")

    print(f"Client: traffic simulated!")
    return kvs
    

def validate_puts(kvs):
    """
        This function will first connect to an etcd host, then perform a get request on each key in the key/value array. 
        For each successful response, we check that the get request value == value from the key/value array. 
        If we ever find a mismatch, we return it. 
    """
    client = helper.connect_to_host()

    for kv in kvs:
        key, value = kv[0], kv[1]
        success, error, database_value = helper.get_request(client, key)

        # Antithesis Assertion: sometimes get requests are successful. A failed request is OK since we expect them to happen.
        sometimes(success, "Client can make successful get requests", {"error":error})

        if not success:
            print(f"Client: unsuccessful get with key '{key}', and error '{error}'")
        else:
            # NEW Antithesis Assertion: if the GET succeeds, it must return the value we previously wrote.
            # This is a stronger, per-operation invariant than the final summary `always(...)` at the end.
            always(
                value == database_value,
                "Read-your-write consistency: successful GET must match PUT",
                {"key": key, "expected": value, "actual": database_value, "error": error},
            )

            if value != database_value:
                print(f"Client: a key value mismatch! This shouldn't happen.")
                return False, (value, database_value)

    print(f"Client: validation ok!")
    return True, None


if __name__ == "__main__":
    kvs = simulate_traffic()
    values_stay_consistent, mismatch = validate_puts(kvs)

    # Antithesis Assertion: for all successful kv put requests, values from get requests should match for their respective keys 
    always(values_stay_consistent, "Database key values stay consistent", {"mismatch":mismatch})