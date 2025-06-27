# OneDep Test Framework

## TODO

- There is object duplication when passing some parameters. A general cleanup is required
- Write unit tests for all modules
- Content type and format check should be performed in WwPDBUri

## Requirements

- For EM it's important to login to the depositions in PRODUCTION so the pickle files are regenerated. I use the pickle files to read the voxel values for file processing

## Testing plan rules

The testing plan file defines settings and rules for testing depositions. Below is a breakdown of its sections.

### `api`

This is to define variables required for connecting to the API and creating depositions.

- **base_url**: Base URL for the deposition API.
- **orcid**: ORCID to create depositions with.
- **country**: Country of operation.

### `remote_archive`

This section defines where the remote archive is locate. This remote archive is where we're going to download depositions from.

It must be accessible by the testing host.

- **host**: Remote archive server hostname.
- **user**: Username for accessing the archive.
- **site_id**: Identifier for the archive site.
- **key_file**: Path to a key file to be used in the SSH connection.

### `compare_rules`

Defines rules for comparing files during the deposition process. Each rule must be in the format `content_type[-milestone].format`. The allowed content types and formats are listed on the [`FileTypeMapping`](odtf/models.py) class.

- **method**: Specifies the comparison method (e.g., `cifdiff`, `jsondiff`).
- **version**: Indicates the version of the file to compare (e.g., `latest`, `original`, numeric).
- **categories**: Lists specific categories for comparison (if applicable). **Not implemented yet.**

```yaml
compare_rules:
  content_type[-milestone].format:
    method: cifdiff|jsondiff|hash
    version: version_str
    categories:
      - category1
```

### `test_set`

Defines test cases. The deposition ID must be the key for each case. Under this key you'll define the tasks to be executed. Generic format:

```yaml
test_set:
  deposition_id:
    tasks:
      - task1
      - task2:
        - task2_options
```

#### Available tasks

- `upload`: Creates a new deposition based on the provided one, uploads the listed files and process them.

```yaml
- upload:
  files:
    - <content_type[-milestone]>.<format>
    - <content_type[-milestone]>.<format>
    ...
```

- `compare_files`: Defines file comparison rules and sources.

The `source` field must contain an URI in the [WwPDBResourceURI](odtf/wwpdb_uri.py) format. If you want to compare with the newly created deposition, use `:copy:` as deposition ID (eg `wwpdb://deposit-ui/:copy:/`). Otherwise, use the deposition ID and repository you want to compare files (eg `wwpdb://archive/D_800001/`).

```yaml
- compare_files:
  source: repository in WwPDBResourceURI format 
  rules:
    - compare_rule1
    - compare_rule2
```

The repository for the deposition ID specified in the test case will ALWAYS be `tempdep`, as this is where I store the files from production archive. In the example below, the files to be compared will be from the `deposit-ui` repository for the newly created deposition and the files for `D_1292137346` in `tempdep`.

```yaml
D_1292137346: # this deposition will be in tempdep
  tasks:
    - upload:
    ...
    - compare_files:
      source: wwpdb://deposit-ui/:copy:/ # this deposition will be the newly created one from the upload task
      rules:
        - model.pdbx
```

- `submit`: Indicates submission tasks.

### Usage

This file is used to configure and automate deposition workflows, including file uploads, comparisons, and submissions. Ensure all paths, rules, and tasks are correctly defined for successful execution.

## Example testing plans

A very short example:

```yaml
api:
  base_url: https://onedep-ann-prod-001:12000/deposition # path to my local API server
  orcid: 0000-0002-5109-8728 # my orcid
  country: United Kingdom # country for depositions

remote_archive: # connection details for one of our annotation VMs to download files from archive
  host: pdb-002.ebi.ac.uk
  user: w3_pdb05
  site_id: PDBE_PROD

compare_rules:
  model.pdbx: # will compare the latest versions of the model in mmCIF format using cifdiff
    method: cifdiff
    version: latest

test_set:
  D_1292133558: # em single particle
    tasks:
      - upload:
        files: # list here all required files for this dep. It still doesn´t support multiple partitions. Working on it
          - model.pdb
          - em-volume.map
          - em-half-volume.map
          - em-half-volume.map
          - img-emdb.jpg
      - compare_files:
        source: wwpdb://deposit-ui/:copy:/ # comparison will be between latest model files in deposit-ui/D_XXX and tempdep/D_1292133558
        rules:
          - model.pdbx
      - submit
```

The [plans/](plans/) folder contain some testing plan examples. Each file should define the structure, steps, and configurations required for specific test scenarios. Use these to write your own.
