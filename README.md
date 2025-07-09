# OneDep Test Framework

This tool is a first attempt to try to test the DepUI in an automated way. It still doesn't support the Annotation side, but it can be extended.

I wrote this in a hurry, so the code is bad! But it will do for now and I hope it can be refactored for a better architecture. What I have in mind is for it to use a proper orchestration framework to provide visibility and task management.

> **Warning**: This package contains bugs. Report them to me so I can patch them while I don't have time to refactor the tool.

> **Warning: Don't ever run this tool in a production environment. I've put some safeguards for PDBe just in case, but it's good to reinforce that.**

## Installation

Clone the repo and run `pip install -U .`

## Usage

You need to have a test server with OneDep installed and a plan YAML ready.

On your test server, run `odtf /path/to/your/plan.yaml`. If your test set is long (3+ entries, lots of tasks), this will take a while to run (half an hour?). Running it in a `tmux` session is advised.

> **Warning: Again. Don't ever run this tool in a production environment.**

## How it works

This tool is centered around testing plans (YAML files) that define a list of depositions to use as a test.

For each deposition listed, you can define tasks to be performed on it. The most common test case is to create a *new* deposition based on a previous one (usually from the production archive), uploading the same files originally uploaded and compare the processed files with the original ones. Another test case is just to run a task on an *existing* deposition in your test installation (e.g. reupload, resubmit it etc).

Take the very short example below.

```yaml
remote_archive: # connection details for one of our annotation VMs to download files from archive
  host: pdb-002.ebi.ac.uk
  user: w3_pdb05
  site_id: PDBE_PROD
  key_file: /home/foo/.ssh/id_ebi

test_set:
  D_1292133558: # em single particle locate at PDBe
    tasks:
      - create
      - upload:
        files: # files to be uploaded. This tool will search these among the files fetched from archive
          - model.pdb
          - em-volume.map
          - em-half-volume.map
          - em-half-volume.map
          - img-emdb.jpg
      - compare_files:
        source: wwpdb://deposit-ui/:copy:/ # comparison will be between latest model files in deposit-ui/D_XXX and tempdep/D_1292133558
        rules:
          - model.pdbx # compare the model file generated (which is located in the deposit-ui repo) with the original one
      - submit
```

The diagram below describes this test case in detail.

![](./docs/testcase_example.svg)

The file comparison rules are defined in a separate section and referenced by the `compare_files.rules` field (check below).

We could perform another file comparison after submission. We could reupload just the model. And of course, we can add more entries.

## Requirements

- For EM it's important to login to the depositions in PRODUCTION so the pickle files are regenerated. I use the pickle files to read the voxel values for file processing

## Testing Plan Sections

The testing plan file defines settings and rules for testing depositions. Below is a breakdown of its sections.

### `api`

This is to define variables required for connecting to the API and creating depositions.

- **base_url**: Base URL for the deposition API. Usually this is the URL you use to access the DepUI.
- **orcid**: ORCiD used to create the depositions.
- **country**: Country of operation.

### `remote_archive`

This section is to define how to access the remote archive from where we'll download the depositions.

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

- `create`: Creates a new deposition based on the provided one.

```yaml
- create
```

- `upload`: Uploads the listed files and process them. All required files MUST be listed. The tool will search for these files in the fetched deposition.

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

- `compare_repos`: Compare two repositories.

This will list files in the two specified repositories and check which ones are missing from the *other*.

The `source` field must contain an URI in the [WwPDBResourceURI](odtf/wwpdb_uri.py) format for the repository you want to compare with. The comparison will happen between the repository for the test case deposition, and the one specified in source. `:copy:` may be used to refer to the newly created deposition.

```yaml
- compare_repos:
  source: repository in WwPDBResourceURI format 
```

Examples:

```yaml
D_8233000142:
  tasks:
    ...
    - compare_repos:
      source: wwpdb://deposit-ui/:copy:/ # will compare deposit-ui from the newly created deposition with deposit-ui from D_8233000142

D_8233000141:
  tasks:
    - compare_repos:
      source: wwpdb://deposit-ui/D_8233000142/ # will compare deposit-ui from D_8233000141 with deposit-ui from D_8233000142
```

- `submit`: Indicates submission tasks.

### Usage

This file is used to configure and automate deposition workflows, including file uploads, comparisons, and submissions. Ensure all paths, rules, and tasks are correctly defined for successful execution.

## Example testing plans

A very short example:

```yaml
api:
  base_url: https://onedep-ann-prod-001:12000/deposition # path to my LOCAL/TEST API server
  orcid: 0000-0002-5109-8728 # my orcid
  country: United Kingdom # country for depositions

report: # this is just to get an url to the report after the test is finished
  depui_url: https://local.rcsb.rutgers.edu:12000/

remote_archive: # connection details for one of our annotation VMs to download files from archive
  host: pdb-002.ebi.ac.uk
  user: w3_pdb05
  site_id: PDBE_PROD
  key_file: /my/key/file

compare_rules:
  model.pdbx: # will compare the latest versions of the model in mmCIF format using cifdiff
    method: cifdiff
    version: latest

test_set:
  D_1292133558: # em single particle
    tasks:
      - upload:
        files: # list here all required files for this dep. If the same content-type.format is used, it will search for additional partitions
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


## TODO

- There is object duplication when passing some parameters. A general cleanup is required
- Write unit tests for all modules
- Content type and format check should be performed in WwPDBUri