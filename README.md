# UK Legislation Downloader & Dataset Creator

This project provides scripts to systematically download the complete UK legislation database from legislation.gov.uk and create comprehensive datasets suitable for LLM fine-tuning.

## Features

- **Comprehensive Coverage**: Downloads all UK legislation types including Acts, Statutory Instruments, and more
- **Multiple Formats**: Downloads XML, HTML, and plain text versions
- **Progress Tracking**: Saves progress and can resume interrupted downloads
- **Verification**: Includes verification round to ensure all files downloaded correctly
- **Rate Limiting**: Respectful downloading with appropriate delays
- **Logging**: Detailed logging of all operations
- **🆕 LLM Dataset Creation**: Creates complete datasets for fine-tuning language models
- **🆕 Multiple Dataset Types**: Instruction-following, text completion, and Q&A datasets
- **🆕 HuggingFace Integration**: Full compatibility with HuggingFace Datasets and Transformers

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Complete Pipeline (Recommended)
```bash
# Run the complete pipeline from download to dataset creation
python complete_pipeline.py
```

### Individual Components

#### Basic Download Only
```bash
python improved_downloader.py
```

#### Dataset Creation Only (from existing files)
```bash
python dataset_creator.py
```

#### Advanced Pipeline Options
```bash
# Skip download, only create datasets
python complete_pipeline.py --skip-download

# Skip dataset creation, only download
python complete_pipeline.py --skip-datasets

# Custom directories
python complete_pipeline.py --output-dir ./my_legislation --dataset-dir ./my_datasets

# Custom tokenizer for processing
python complete_pipeline.py --tokenizer "microsoft/DialoGPT-large"
```

### What the complete pipeline does:

1. **Discovery Phase**: Systematically discovers all UK legislation by:
   - Browsing by legislation type and year
   - Using search interfaces
   - Removing duplicates

2. **Download Phase**: Downloads each piece of legislation in multiple formats:
   - XML format (`/data.xml`)
   - HTML format (main page)
   - Plain text (extracted from HTML)
   - Metadata (JSON)

3. **Verification Phase**: 
   - Checks all files were downloaded
   - Identifies missing or corrupted files
   - Retries failed downloads
   - Generates final statistics

4. **🆕 Dataset Creation Phase**: Creates comprehensive LLM training datasets:
   - **Instruction-Following Dataset**: Question-answer pairs and instruction-response formats
   - **Text Completion Dataset**: Chunked text suitable for pre-training style fine-tuning
   - **Q&A Dataset**: Structured question-answer pairs from legislation content
   - **Combined Training Dataset**: All datasets with train/validation splits
   - **Multiple Formats**: HuggingFace Dataset, Parquet, and JSON formats

## Output Structure

### Downloaded Legislation Files
```
uk_legislation/
├── xml/              # XML format files
├── html/             # HTML format files  
├── text/             # Plain text files
├── metadata/         # JSON metadata files
├── discovered_legislation.json    # List of all found legislation
├── progress.json                  # Download progress tracking
├── verification_report.json       # Final verification results
└── legislation_download.log       # Detailed operation log
```

### 🆕 Created Datasets
```
uk_legislation_datasets/
├── raw/                          # Raw dataset files
│   ├── legislation_raw/          # HuggingFace Dataset format
│   ├── legislation_raw.parquet   # Parquet format
│   └── legislation_raw.json      # JSON format
├── processed/                    # Processed dataset files
│   ├── legislation_instructions/ # Instruction-following dataset
│   ├── legislation_completion/   # Text completion dataset
│   ├── legislation_qa/           # Q&A dataset
│   └── *.parquet                # Parquet versions
├── final/                        # Final training datasets
│   ├── uk_legislation_complete/  # Combined dataset (HuggingFace format)
│   ├── instruction_following_train.parquet
│   ├── instruction_following_validation.parquet
│   ├── text_completion_train.parquet
│   ├── text_completion_validation.parquet
│   ├── question_answering_train.parquet
│   ├── question_answering_validation.parquet
│   └── validation_report.json    # Dataset statistics and validation
├── dataset_creation.log          # Dataset creation log
└── validation_report.json        # Overall validation report
```

## Legislation Types Covered

- UK Public General Acts (ukpga)
- UK Local Acts (ukla)
- UK Statutory Instruments (uksi)
- UK Draft Statutory Instruments (ukdsi)
- Acts of the Scottish Parliament (asp)
- Scottish Statutory Instruments (ssi)
- Acts of Senedd Cymru (asc)
- Welsh Statutory Instruments (wsi)
- Acts of the National Assembly for Wales (anaw)
- Measures of the National Assembly for Wales (mwa)
- Northern Ireland Acts (nia)
- Northern Ireland Statutory Rules (nisr)
- UK Ministerial Orders (ukmo)
- UK Draft Ministerial Orders (ukdmo)
- UK Statutory Rules and Orders (uksro)
- Northern Ireland Statutory Rules and Orders (nisro)
- UK Church Instruments (ukci)
- UK Church Measures (ukcm)
- EU Legislation (eur)
- EU Directives (eudr)
- EU Decisions (eurdn)

## Progress Tracking

The script automatically saves progress and can be interrupted and resumed. Progress is saved every 50 downloads and includes:

- List of successfully downloaded items
- List of failed items
- Timestamp of last save

## Rate Limiting

The script includes respectful rate limiting:
- 1-2 second delays between downloads
- Longer delays for discovery operations
- Timeout handling for network requests

## Error Handling

- Network errors are logged and retried
- Corrupted files are detected and re-downloaded
- Missing files are identified in verification
- All errors are logged with detailed information

## Resuming Downloads

To resume an interrupted download, simply run the script again. It will:
1. Load previous progress
2. Skip already downloaded items
3. Continue from where it left off

## Legal and Ethical Considerations

- This script accesses publicly available UK legislation data
- Downloads are performed respectfully with rate limiting
- Content is available under Crown Copyright
- For educational and research purposes

## Troubleshooting

Check the log file `legislation_download.log` for detailed error information.

Common issues:
- **Network timeouts**: Script will retry automatically
- **Disk space**: Ensure sufficient space (legislation database is large)
- **Rate limiting**: Built-in delays should prevent this

## 🆕 Using the Datasets for LLM Training

### Loading Datasets
```python
from datasets import load_from_disk, load_dataset

# Load the complete combined dataset
dataset = load_from_disk("uk_legislation_datasets/final/uk_legislation_complete")

# Load specific splits
train_data = dataset["instruction_following_train"]
val_data = dataset["instruction_following_validation"]

# Or load from Parquet files
train_data = load_dataset("parquet", 
    data_files="uk_legislation_datasets/final/instruction_following_train.parquet")
```

### Training Examples

#### Instruction-Following Fine-tuning
```python
# Each example contains:
# - instruction: The task or question
# - input: Additional context (often empty)
# - output: The expected response
# - legislation_id, legislation_type, legislation_year: Metadata

example = {
    "instruction": "What is the Consumer Rights Act 2015?",
    "input": "",
    "output": "The Consumer Rights Act 2015 is a UK Public General Act...",
    "legislation_id": "ukpga_2015_15",
    "legislation_type": "ukpga",
    "legislation_year": "2015"
}
```

#### Text Completion Training
```python
# Each example contains chunked legislation text
# suitable for completion-style training

example = {
    "text": "PART 1\nCONSUMER CONTRACTS FOR GOODS...",
    "legislation_id": "ukpga_2015_15",
    "legislation_type": "ukpga",
    "legislation_year": "2015"
}
```

#### Question-Answering Training
```python
# Structured Q&A pairs about legislation content

example = {
    "question": "What does section 1 of the Consumer Rights Act 2015 state?",
    "answer": "Section 1 states that this Part applies to...",
    "context": "Full legislation text for context...",
    "legislation_id": "ukpga_2015_15"
}
```

### Training Recommendations

1. **For General Legal Knowledge**: Use the instruction-following dataset
2. **For Text Generation**: Use the text completion dataset  
3. **For Legal Q&A Systems**: Use the question-answering dataset
4. **For Comprehensive Understanding**: Combine all datasets

### Dataset Statistics

The validation report (`validation_report.json`) provides detailed statistics:
- Total examples per dataset type
- Text length distributions
- Coverage by legislation type and year
- Quality metrics

## Output Statistics

After completion, the complete pipeline provides detailed statistics:
- Total legislation items discovered and downloaded
- Number of files downloaded by format
- Missing or corrupted files
- Dataset creation metrics and validation results
- Overall success rate