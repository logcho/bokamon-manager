# bokamon-manager

## Overview

This program manages player and match data for the Bokamon card game using a MySQL database. It supports table creation, player registration, match tracking, and statistical queries, following the CS 5330/7330 Fall 2025 programming assignment specifications.

## Database Setup

1. Start MySQL and log in as root:

   ```bash
   mysql -u root
   ```
2. Create the database and user:

   ```sql
   DROP DATABASE IF EXISTS dbprog;
   CREATE DATABASE dbprog;
   CREATE USER IF NOT EXISTS 'cs5330'@'localhost' IDENTIFIED BY 'pw5330';
   GRANT ALL PRIVILEGES ON dbprog.* TO 'cs5330'@'localhost';
   FLUSH PRIVILEGES;
   EXIT;
   ```

## Running the Program

1. Ensure MySQL is running:

   ```bash
   brew services start mysql
   ```
2. Run the manager script:

   ```bash
   python3 bokamon-manager.py
   ```
3. When prompted, enter the input file path, for example:

   ```
   Enter input CSV filename: testcase/test1.txt
   ```

## Supported Commands

| Command | Description                                                                                                     |
| ------- | --------------------------------------------------------------------------------------------------------------- |
| `e`     | Create or reset both tables (drops existing ones).                                                              |
| `r`     | Clear all data while keeping tables.                                                                            |
| `p`     | Add a player (`ID, Name, Birthdate(YYYYMMDD), Rating, State`).                                                  |
| `m`     | Record a completed match (`HostID, GuestID, Start, End, HostWin(1/0), PreHost, PostHost, PreGuest, PostGuest`). |
| `n`     | Schedule an unplayed match (`HostID, GuestID, Start`).                                                          |
| `c`     | Complete a previously scheduled match (same format as `m`).                                                     |
| `P`     | Display information for one player.                                                                             |
| `A`     | Show aggregated win/loss statistics for a list of players.                                                      |
| `D`     | List completed matches between two dates.                                                                       |
| `M`     | List all matches involving a player, flagging inconsistent ratings.                                             |

## Constraints and Error Handling

* Duplicate IDs, overlapping matches, or invalid foreign keys result in the input line followed by `Input Invalid`.
* The program enforces:

  * Unique player IDs and names.
  * Referential integrity between players and matches.
  * No player can participate in overlapping matches.
  * Valid date and time formats.

## Example

**Input (testcase/test1.txt):**

```
e
p,111111,John Doe,20000101,1000,TX
p,222222,Jack Doe,20000202,1200,CA
p,333333,Jim Doe,20000303,900,MD
m,111111,222222,20250209:18:37:45,20250209:19:37:00,1,1000,1010,1200,1199
P,111111
D,20250201,20250301
A,111111,222222,333333
```

**Output:**

```
111111 John Doe 20000101 1010 TX
20250209:18:37:45 20250209:19:37:00 John Doe Jack Doe H
333333 Jim Doe 1 0 1.0000
111111 John Doe 1 1 0.5000
222222 Jack Doe 1 2 0.3333
```

## Notes

* Always start test files with `e` to create tables fresh.
* The database name is `dbprog`, user is `cs5330`, password is `pw5330`.
* The program rolls back invalid transactions and prints only valid results.
