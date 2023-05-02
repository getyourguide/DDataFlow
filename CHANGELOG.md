# 1.1.15

- Disable logs when DDataflow is not enable. Support setting logger level.

# 1.1.14

- Fix sample and download function

# 1.1.13

- Support s3 path and default database

# 1.1.9

- Upgrade dependencies due to security
 
# 1.1.8

- Release new version with security patch on urllib

# 1.1.7

- Fix bug in source_name.

# 1.1.6 

- Security dependency fix

# 1.1.5  - 2022-10-12

- Allow customization of default sampler limit

# 1.1.4  - 2022-10-11

- improved initial project config
 
# 1.1.3  - 2022-10-11

- is_enabled api added
- add documentation website

# 1.1.2  - 2022-10-05

- is_local api added

# 1.1.0  - 2022-09-30

- Support default sampling enablement in the sources configuration
  - Use default_sample: True
- Support omitting the source name in the sources configuration
  - When omitted assumes table and uses the source key as the table name

# 1.0.2  - 2022-09-29

- Loosen up dependencies

# 1.0.1  - 2022-09-06

- Upgrade dependencies for security reasons

# 1.0.0  - 2022-08-26

- Add missing fire dependency and graduate project to 1.0

# 0.2.0

- Fix bug while downloading data sources

# 0.1.12

- Better error messages and documentation

# 0.1.10

- Support ddataflow.path to only replace the path in an environment

# 0.1.9

- Refactorings
- Fix setup command GB to int

# 0.1.8

- Fix missing creating tmp view
- Improve messages indentation

# 0.1.7

- Fix source_name for offline mode
- Fix tests

# 0.1.6

- Make examples simpler
- Use ddataflow instead of ddataflow_object in the setup examples

# 0.1.5

- Improve documentation and add an example
- Improve the setup_project options and documentation

# 0.1.4

- create setup_project command
- use limit only rather than sampling for default sampled entities
