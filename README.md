# baseball.computer
This is the library that powers the [baseball.computer](https://baseball.computer) database.

Starting from a set of Retrosheet files dropped off by the [Rust parser](https://github.com/droher/baseball.computer.rs), this library builds out a duckdb SQL database using [dbt](https://github.com/dbt-labs/dbt-core).

## Quick Start

### Prerequisites

- Python 3.11+
- Rust and Cargo (for the Retrosheet parser)
- duckdb Python package

### Setup

1. **Clone both repositories** (the Rust parser is a separate repo):

```bash
# Clone the main repository
git clone https://github.com/droher/baseball.computer.git
cd baseball.computer

# Clone the Rust parser alongside
git clone https://github.com/droher/baseball.computer.rs.git
```

2. **Build the Rust parser**:

```bash
cd baseball.computer.rs
cargo build --release
cd ..
```

3. **Install Python dependencies**:

```bash
python3 -m venv venv313
source venv313/bin/activate  # On Windows: venv313\Scripts\activate
pip install duckdb
```

4. **Build the database**:

```bash
python3 build_all.py
```

### Database Location

The database is created at `baseball.duckdb` in the project root.

## Documentation

- [Database documentation](https://docs.baseball.computer)
- [Build process documentation](README_BUILD_ALL.md)
- [Historical data import](README_PROCESS_HISTORICAL.md)