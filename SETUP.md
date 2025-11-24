# Setup Instructions for Toyota Gazoo Racing dbt Project

## Issue Found

Your `~/.zshrc` file has a `DBT_PROJECT_DIR` environment variable set that was forcing dbt to always use another project.

## Fix Required

**Edit your `~/.zshrc` file** and comment out line 29:

```bash
# Open your shell config
code ~/.zshrc
# or
nano ~/.zshrc
```

Find line 29 and change from:
```bash
export DBT_PROJECT_DIR="/Users/andreamontes/Documents/qventus/standard-solution-views"
```

To:
```bash
# Commented out to allow working with multiple dbt projects
# export DBT_PROJECT_DIR="/Users/andreamontes/Documents/qventus/standard-solution-views"
```

Then reload your shell:
```bash
source ~/.zshrc
```

## Alternative: Project-Specific Environment

If you need `DBT_PROJECT_DIR` for the other project, you can:

1. Remove it from `~/.zshrc` globally
2. Create a `.envrc` file in each project directory using [direnv](https://direnv.net/)
3. Or create project-specific aliases in your `.zshrc`:

```bash
# Add to ~/.zshrc instead of the export
alias dbt-qventus='DBT_PROJECT_DIR="/Users/andreamontes/Documents/qventus/standard-solution-views" dbt'
alias dbt-toyota='cd /Users/andreamontes/Documents/personal/toyota_gazoo && dbt'
```

## Verify Setup

After fixing the environment variable, verify dbt works:

```bash
cd /Users/andreamontes/Documents/personal/toyota_gazoo
dbt debug
```

You should see:
- ✅ `profiles.yml file [OK found and valid]`
- ✅ `dbt_project.yml file [OK found and valid]`
- ✅ `Connection test: [OK connection ok]`
- ✅ `All checks passed!`

## Next Steps

1. Install dbt packages:
   ```bash
   dbt deps
   ```

2. Start building your models in `models/staging/`

3. Run your first model:
   ```bash
   dbt run
   ```

## Project Structure Created

```
toyota_gazoo/
├── dbt_project.yml          # Project configuration
├── packages.yml             # dbt package dependencies
├── README.md                # Project documentation
├── models/
│   ├── staging/            # Raw data transformations
│   │   └── _staging.yml    # Source definitions
│   ├── intermediate/       # Business logic layers
│   └── marts/              # Final analytical models
├── analyses/               # Ad-hoc queries and audits
├── seeds/                  # Reference data CSVs
├── macros/                 # Reusable SQL macros
├── snapshots/              # Type-2 SCD tables
└── tests/                  # Custom data tests
```

