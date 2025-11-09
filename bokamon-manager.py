import csv
import sys
from io import StringIO
from datetime import datetime, date
from decimal import Decimal, ROUND_HALF_UP
import mysql.connector as mysql
from mysql.connector import Error as MySQLError


DSN = {
    "user": "cs5330",
    "password": "pw5330",
    "host": "localhost",
    "database": "dbprog",
    "autocommit": False,
}


def one_space_join(*fields):
    return " ".join(str(f) for f in fields)


def fmt_date(d: date) -> str:
    return d.strftime("%Y%m%d")


def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y%m%d:%H:%M:%S")


def parse_yyyymmdd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()


def parse_yyyymmdd_hhmmss(s: str) -> datetime:
    return datetime.strptime(s, "%Y%m%d:%H:%M:%S")


def round4(x: float) -> str:
    return f"{Decimal(x).quantize(Decimal('0.0001'), rounding=ROUND_HALF_UP):.4f}"


CREATE_PLAYER = """
CREATE TABLE Player (
    ID CHAR(6) PRIMARY KEY,
    Name VARCHAR(255) UNIQUE NOT NULL,
    Birthdate DATE NOT NULL,
    Rating INT NOT NULL,
    State CHAR(2) NOT NULL
) ENGINE=InnoDB;
"""


CREATE_MATCHES = """
CREATE TABLE Matches (
    MatchID BIGINT AUTO_INCREMENT PRIMARY KEY,
    HostID CHAR(6) NOT NULL,
    GuestID CHAR(6) NOT NULL,
    Start DATETIME NOT NULL,
    EndTime DATETIME NULL,
    HostWin TINYINT(1) NULL,
    PreRatingHost INT NULL,
    PostRatingHost INT NULL,
    PreRatingGuest INT NULL,
    PostRatingGuest INT NULL,
    INDEX idx_host_time (HostID, Start),
    INDEX idx_guest_time (GuestID, Start),
    CONSTRAINT fk_host FOREIGN KEY (HostID) REFERENCES Player(ID)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_guest FOREIGN KEY (GuestID) REFERENCES Player(ID)
        ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB;
"""


class DB:
    def __init__(self):
        self.conn = mysql.connect(**DSN)

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def ensure(self):
        cur = self.conn.cursor()
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.execute("DROP TABLE IF EXISTS Matches")
        cur.execute("DROP TABLE IF EXISTS Player")
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        cur.execute(CREATE_PLAYER)
        cur.execute(CREATE_MATCHES)
        self.conn.commit()
        cur.close()

    def player_exists(self, pid):
        cur = self.conn.cursor()
        cur.execute("SELECT 1 FROM Player WHERE ID=%s", (pid,))
        ok = cur.fetchone()
        cur.close()
        return bool(ok)

    def player_name(self, pid):
        cur = self.conn.cursor()
        cur.execute("SELECT Name FROM Player WHERE ID=%s", (pid,))
        row = cur.fetchone()
        cur.close()
        return row[0] if row else None

    def conflict(self, pid, start, end, exclude=None):
        cur = self.conn.cursor()
        sql = [
            "SELECT MatchID, Start, EndTime FROM Matches WHERE (HostID=%s OR GuestID=%s)"
        ]
        params = [pid, pid]

        if exclude:
            sql.append("AND MatchID<>%s")
            params.append(exclude)

        cur.execute(" ".join(sql), tuple(params))

        for mid, s, e in cur.fetchall():
            if e is None:
                if end is None:
                    if s == start:
                        cur.close()
                        return True
                elif start <= s < end:
                    cur.close()
                    return True
            else:
                if end is None:
                    if s <= start < e:
                        cur.close()
                        return True
                elif start < e and end > s:
                    cur.close()
                    return True

        cur.close()
        return False

    def add_player(self, pid, name, bday, rating, state):
        try:
            cur = self.conn.cursor()
            cur.execute(
                "INSERT INTO Player VALUES(%s,%s,%s,%s,%s)",
                (pid, name, bday, rating, state),
            )
            self.conn.commit()
            cur.close()
            return True
        except MySQLError as e:
            # print("MySQL player insert error:", e)
            self.conn.rollback()
            return False

    def insert_completed(self, host, guest, start, end, hostwin, prh, poh, prg, pog, mid=None):
        if not (self.player_exists(host) and self.player_exists(guest)):
            return False

        if host == guest or end < start:
            return False

        if self.conflict(host, start, end, mid) or self.conflict(guest, start, end, mid):
            return False

        try:
            cur = self.conn.cursor()

            if mid is None:
                cur.execute(
                    """
                    INSERT INTO Matches(
                        HostID, GuestID, Start, EndTime, HostWin,
                        PreRatingHost, PostRatingHost, PreRatingGuest, PostRatingGuest
                    ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (host, guest, start, end, int(hostwin), prh, poh, prg, pog),
                )
            else:
                cur.execute(
                    """
                    UPDATE Matches
                    SET EndTime=%s, HostWin=%s, PreRatingHost=%s, PostRatingHost=%s,
                        PreRatingGuest=%s, PostRatingGuest=%s
                    WHERE MatchID=%s
                    """,
                    (end, int(hostwin), prh, poh, prg, pog, mid),
                )

            cur.execute("UPDATE Player SET Rating=%s WHERE ID=%s", (poh, host))
            cur.execute("UPDATE Player SET Rating=%s WHERE ID=%s", (pog, guest))

            self.conn.commit()
            cur.close()
            return True

        except MySQLError as e:
            print("MySQL error inserting match:", e)
            self.conn.rollback()
            return False

    def insert_scheduled(self, host, guest, start):
        if not (self.player_exists(host) and self.player_exists(guest)):
            return False

        if host == guest or self.conflict(host, start, None) or self.conflict(guest, start, None):
            return False

        try:
            cur = self.conn.cursor()
            cur.execute(
                "INSERT INTO Matches(HostID, GuestID, Start) VALUES(%s,%s,%s)",
                (host, guest, start),
            )
            self.conn.commit()
            cur.close()
            return True

        except MySQLError as e:
            print("MySQL scheduled match error:", e)
            self.conn.rollback()
            return False

    def complete_existing(self, host, guest, start, end, hw, prh, poh, prg, pog):
        cur = self.conn.cursor()
        cur.execute(
            "SELECT MatchID FROM Matches WHERE HostID=%s AND GuestID=%s AND Start=%s AND HostWin IS NULL",
            (host, guest, start),
        )
        r = cur.fetchone()
        cur.close()

        if not r:
            return False

        return self.insert_completed(host, guest, start, end, hw, prh, poh, prg, pog, mid=r[0])

    def print_player(self, pid):
        cur = self.conn.cursor()
        cur.execute("SELECT ID, Name, Birthdate, Rating, State FROM Player WHERE ID=%s", (pid,))
        r = cur.fetchone()
        cur.close()

        if r:
            print(one_space_join(r[0], r[1], fmt_date(r[2]), r[3], r[4]))

    def list_matches_in_range(self, sd, ed):
        sdt = datetime.combine(sd, datetime.min.time())
        edt = datetime.combine(ed, datetime.max.time())

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT Start, EndTime, HostID, GuestID, HostWin
            FROM Matches
            WHERE HostWin IS NOT NULL AND Start BETWEEN %s AND %s
            ORDER BY Start, HostID
            """,
            (sdt, edt),
        )

        for s, e, h, g, hw in cur.fetchall():
            print(
                one_space_join(
                    fmt_dt(s), fmt_dt(e), self.player_name(h), self.player_name(g), 'H' if hw else 'G'
                )
            )

        cur.close()

    def list_aggregate(self, pids):
        seen = set()
        valid = []

        for p in pids:
            if p not in seen and self.player_exists(p):
                seen.add(p)
                valid.append(p)

        if len(valid) <= 1:
            return

        cur = self.conn.cursor()
        fmt = ",".join(["%s"] * len(valid))

        cur.execute(
            f"SELECT HostID, GuestID, HostWin FROM Matches WHERE HostWin IS NOT NULL AND HostID IN ({fmt}) AND GuestID IN ({fmt})",
            tuple(valid + valid),
        )

        stats = {p: [0, 0] for p in valid}

        for h, g, hw in cur.fetchall():
            if hw:
                stats[h][0] += 1
                stats[g][1] += 1
            else:
                stats[h][1] += 1
                stats[g][0] += 1

        cur.close()

        arr = []
        for p, (w, l) in stats.items():
            pct = (w / (w + l) * 100) if (w + l) else 0
            arr.append((p, self.player_name(p), w, l, pct))

        arr.sort(key=lambda x: (-x[4], -x[2], x[0]))

        for p, n, w, l, pct in arr:
            print(one_space_join(p, n, w, l, round4(pct)))

    def list_player_matches(self, pid):
        n = self.player_name(pid)
        if not n:
            return

        print(one_space_join(pid, n))

        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT Start, EndTime, HostID, GuestID, HostWin,
                   PreRatingHost, PostRatingHost, PreRatingGuest, PostRatingGuest
            FROM Matches
            WHERE HostID=%s OR GuestID=%s
            ORDER BY Start, HostID
            """,
            (pid, pid),
        )

        last = None

        for s, e, h, g, hw, prh, poh, prg, pog in cur.fetchall():
            opp = g if h == pid else h
            oppn = self.player_name(opp)

            res = ''
            pre = None
            post = None

            if hw is not None:
                if h == pid:
                    res = 'W' if hw else 'L'
                    pre = prh
                    post = poh
                else:
                    res = 'W' if not hw else 'L'
                    pre = prg
                    post = pog

            line = [fmt_dt(s), fmt_dt(e) if e else '', opp, oppn, res, post or '']

            if hw is not None and pre is not None and last is not None and pre != last:
                line.append('inconsistent rating')

            print(one_space_join(*line))

            if post is not None:
                last = post

        cur.close()


def process_file(db, fname):
    with open(fname) as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue

            reader = csv.reader(StringIO(line))

            try:
                fields = next(reader)
            except Exception:
                print(line + " Input Invalid")
                continue

            cmd = fields[0].strip() if fields else ''

            try:
                if cmd == 'e':
                    db.ensure()

                elif cmd == 'p':
                    pid, name, bday, rating, state = fields[1:6]
                    if not db.add_player(pid, name, parse_yyyymmdd(bday), int(rating), state):
                        print(line + " Input Invalid")

                elif cmd == 'm':
                    host, guest, st, en, hw, prh, poh, prg, pog = fields[1:10]
                    if not db.insert_completed(
                        host,
                        guest,
                        parse_yyyymmdd_hhmmss(st),
                        parse_yyyymmdd_hhmmss(en),
                        bool(int(hw)),
                        int(prh),
                        int(poh),
                        int(prg),
                        int(pog),
                    ):
                        print(line + " Input Invalid")

                elif cmd == 'n':
                    host, guest, st = fields[1:4]
                    if not db.insert_scheduled(host, guest, parse_yyyymmdd_hhmmss(st)):
                        print(line + " Input Invalid")

                elif cmd == 'c':
                    host, guest, st, en, hw, prh, poh, prg, pog = fields[1:10]
                    if not db.complete_existing(
                        host,
                        guest,
                        parse_yyyymmdd_hhmmss(st),
                        parse_yyyymmdd_hhmmss(en),
                        bool(int(hw)),
                        int(prh),
                        int(poh),
                        int(prg),
                        int(pog),
                    ):
                        print(line + " Input Invalid")

                elif cmd == 'P':
                    db.print_player(fields[1])

                elif cmd == 'A':
                    db.list_aggregate(fields[1:])

                elif cmd == 'D':
                    db.list_matches_in_range(
                        parse_yyyymmdd(fields[1]),
                        parse_yyyymmdd(fields[2])
                    )

                elif cmd == 'M':
                    db.list_player_matches(fields[1])

            except Exception:
                if cmd in {'p', 'm', 'n', 'c'}:
                    print(line + " Input Invalid")
                db.conn.rollback()


def main():
    try:
        db = DB()
    except MySQLError as e:
        print('Failed to connect to MySQL:', e)
        sys.exit(1)

    try:
        fname = input('Enter input CSV filename: ').strip()
        process_file(db, fname)
    finally:
        db.close()


if __name__ == '__main__':
    main()