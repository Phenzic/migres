# migres
An AI package for migrating data from mariadb to postgres directly


## Configuration

After installation, create these config files:

1. Create a `.env` file with your database credentials:
```bash
cp .env.example .env
nano .env
```

2. Generate Config templates
```bash
migres init
```

3. Edit the generated config files:
```bash
 type_config.ini
 uuid_config.ini
 table_schema.ini
 constraints.ini
```

4. Environment Variables

```bash
MARIADB_HOST=localhost
MARIADB_USER=root
MARIADB_PASSWORD=yourpassword 
MARIADB_DATABASE=source_db
SUPABASE_CONNECTION_STRING=postgresql://user:password@host:5432/db
```

## Usage

