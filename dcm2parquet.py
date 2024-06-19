# Helper function to get the description of a DICOM tags
def get_tag_description(tag, description):
    """ Get the description of the DICOM tag """
    try:
        return dd.get_entry(tag)[2]  # Get the third element which is the keyword/description
    except KeyError:
        return description

# Sanitize column or field name for compatibility
def sanitize_name(name):
    """ Sanitize column or field name """
    # Remove special characters and replace underscores with an empty string
    return name.replace('(', '').replace(')', '').replace(',', '').replace(' ', '').replace('_', '')

# Convert pydicom sequence item to Python native format
def convert_sequence_item(item):
    """ Convert pydicom sequence item to Python native format """
    return {sanitize_name(get_tag_description(elem.tag, elem.description())): convert_value(elem.value) for elem in item}

# Clean column name by removing special characters and spaces
def clean_column_name(column_name):
    """ Clean column name """
    return ''.join(e for e in column_name if e.isalnum())

# Serialize complex DICOM elements to JSON string while preserving nesting
def serialize_element(value):
    """ Serialize complex DICOM elements to JSON string while preserving nesting """
    if isinstance(value, pydicom.Dataset):
        # Convert the Dataset to a dict preserving the nested structure
        return {sanitize_name(get_tag_description(elem.tag, elem.description())): serialize_element(elem.value) for elem in value}
    elif isinstance(value, pydicom.sequence.Sequence):
        # Convert the Sequence to a list preserving the nested structure
        return [serialize_element(item) for item in value]
    else:
        return convert_value(value)

# Extract DICOM header data and serialize complex types while preserving nesting
def extract_dicom_header(dicom_file):
    """ Extract DICOM header data and serialize complex types while preserving nesting """
    ds = pydicom.dcmread(dicom_file, stop_before_pixels=True)
    header_data = {}
    for elem in ds:
        header_data[sanitize_name(clean_column_name(get_tag_description(elem.tag, elem.description())))] = serialize_element(elem.value)
    return header_data

# Save DICOM header data to a Parquet file
def save_dicom_header_to_parquet(dicom_files, parquet_file):
    """ Save DICOM header data to a Parquet file """
    all_header_data = []
    for dicom_file in dicom_files:
        header_data = extract_dicom_header(dicom_file)
        all_header_data.append(header_data)
    
    df = pd.DataFrame(all_header_data)
    # Sanitize column names
    df.columns = [sanitize_name(col) for col in df.columns]
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_file)

# Example usage
dir_path = Path('/content/test/nlst/206409/1.3.6.1.4.1.14519.5.2.1.7009.9004.254602318574256170675440761676/SEG_1.2.276.0.7230010.3.1.3.313263360.83953.1706310481.875034/')
#dir_path = Path('/content/test/nlst/206409/1.3.6.1.4.1.14519.5.2.1.7009.9004.254602318574256170675440761676/CT_1.3.6.1.4.1.14519.5.2.1.7009.9004.188185458178682635323247716867/')
#dir_path =  Path('/content/test/nlst/206409/1.3.6.1.4.1.14519.5.2.1.7009.9004.254602318574256170675440761676/SR_1.2.276.0.7230010.3.1.3.313263360.92780.1706310606.192321')
#dir_path = Path('/content/test/cptac_ucec/C3L-00006/2.25.71450510815914968006753073988527088751/SM_1.3.6.1.4.1.5962.99.1.288694722.760207494.1640966234562.2.0/')

dicom_files = [str(file) for file in dir_path.glob('*.dcm')]
parquet_file = 'dicom_headers_seg.parquet'
save_dicom_header_to_parquet(dicom_files, parquet_file)
polars.read_parquet('dicom_headers_seg.parquet')


