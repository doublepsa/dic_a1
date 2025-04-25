# dic_a1

Data-intensive Computing - Assignment 1

### Setup

1. Log in to the TU VPN
2. Connect to lbd cluster with `ssh e{yourStudentID}@lbd.tuwien.ac.at`
3. In your ssh session, create a directory for the assignment:

```bash
mkdir DIC2025_Assignment1
cd DIC2025_Assignment1
```

4. Back in your local terminal, upload the 2 asset files to the hadoop cluster:

```bash
scp data/reviews_devset.json e{yourStudentID}@lbd.tuwien.ac.at:~/DIC2025_Assignment1/
scp data/stopwords.txt e{yourStudentID}@lbd.tuwien.ac.at:~/DIC2025_Assignment1/
```

### Run

> [!IMPORTANT]  
> The script ```run_job.sh``` assumes that all the files are within its folder.

Development mode (small local dataset)
```bash
.src()/run_job.sh --dev
```

Debug flag for timing -> creates a debug{_dev}.txt (can be used with final submission and dev)
```bash
./run_job.sh --debug
```

Full Hadoop submission
```bash
./run_job.sh
```
