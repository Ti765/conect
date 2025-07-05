# Firebase Studio

This is a NextJS starter in Firebase Studio.

To get started, take a look at src/app/page.tsx.

## ODBC setup

The development environment relies on the Microsoft ODBC driver. When entering the `nix-shell`, the `shellHook` now exports `ODBCINI` and `ODBCINSTINI` pointing to the generated configuration files. Ensure you run all commands inside the nix shell so `pyodbc` can locate the driver.