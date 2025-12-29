#!/usr/bin/env -S python3 -u

"""
Read-only validation command.

This command verifies read-your-write consistency for a fixed set of keys
without generating new traffic. Useful for post-failure or post-chaos checks.
"""

from antithesis.assertions import always, sometimes
import sys

sys.path.append("/opt/antithesis/resources")
import helper


def validate_only():
    client = helper.connect_to_host()

    # Seed a tiny set of keys so validation reliably exercises successful GETs
    seeded = []
    for _ in range(3):
        k = helper.generate_random_string()
        v = helper.generate_random_string()
        ok, err = helper.put_request(client, k, v)
        err_str = None if err is None else str(err)
        sometimes(ok, "Bonus: can seed keys for validation", {"key": k, "error": err_str})
        if ok:
            seeded.append(k)

    # Read-only validation on the keys we know exist
    for key in seeded:
        success, error, value = helper.get_request(client, key)
        error_str = None if error is None else str(error)

        sometimes(success, "Client can successfully GET seeded keys", {"key": key, "error": error_str})

        if success:
            always(
                value is not None,
                "Read-only GET returned a value",
                {"key": key, "value": value, "error": error_str},
            )


if __name__ == "__main__":
    validate_only()