# Legal LLaMA Datasets

A comprehensive legal data collection and processing framework for UK legal documents. This project provides unified pipelines to systematically collect UK legislation, case law, tax documentation, and housing-related legal content, then process it into datasets suitable for LLM training.

## 🚀 Features

- **Multi-source Data Collection**: UK legislation, case law (BAILII), HMRC tax documentation, housing law
- **Government Content API Integration**: Fast, reliable data extraction using official UK Gov Content API
- **Multiple Output Formats**: XML, HTML, text, structured JSON with comprehensive metadata
- **Progress Tracking & Resume**: Robust state management with automatic resume capabilities
- **Rate Limiting**: Respectful data collection with appropriate delays
- **Comprehensive Logging**: Detailed operation logs in dedicated logs directory
- **LLM Dataset Creation**: Generates instruction-following, text completion, and Q&A datasets
- **HuggingFace Integration**: Full compatibility with HuggingFace Datasets and Transformers
- **Unified CLI Interface**: Single entry point for all data collection pipelines

## 📁 Project Structure

```
legal-llama-datasets/
├── main.py                 # Main CLI entry point
├── pipelines/              # Data collection pipelines
│   ├── hmrc_scraper.py     # HMRC tax documentation scraper
│   ├── bailii_scraper.py   # BAILII case law scraper
│   ├── housing_pipeline.py # Housing legislation & case law
│   ├── complete_pipeline.py# Complete data collection pipeline
│   └── housing_QA_generator.py # Q&A pair generation
├── utils/                  # Utility scripts and tools
│   ├── dataset_creator.py  # LLM dataset creation
│   ├── multi_database_ingestion.py # Database ingestion
│   ├── uk_legislation_downloader.py # UK legislation collection
│   └── improved_downloader.py # Enhanced downloaders
├── tests/                  # Test suite
├── logs/                   # Application logs (gitignored)
├── generated/              # Generated datasets and content (gitignored)
└── requirements.txt        # Python dependencies
```

## 🛠️ Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

## 🎯 Quick Start

### Unified CLI Interface
The main entry point provides access to all data collection pipelines:

```bash
# Show all available pipelines
python main.py --help

# Collect HMRC tax documentation (with Content API)
python main.py hmrc --max-documents 100 --output-dir generated/hmrc_data

# Collect housing legislation and case law
python main.py housing --max-documents 500

# Scrape BAILII case law
python main.py bailii --output-dir generated/case_law

# Run complete data collection pipeline
python main.py complete --max-documents 1000

# Generate Q&A pairs from collected data
python main.py qa-generator --input-dir generated/housing_legislation

# Ingest data into databases
python main.py db-ingestion --input-dir generated/
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