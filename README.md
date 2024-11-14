The script extract the information of numerous dicom files and show them in a .csv file.

This script help you to do at least three things with you imgaing processing:
1. Quickly find out what kind of image data is included.
2. Access to the imaging data related to processing without complicated manual operation
3. Checking the consistency of imaging data and demographic data, preventing errors due to mis-labeling.

Input : 
1. Absolute path of dicom dataset as source_dir
2. Research ID for subjects (e.g. BP001, BP002... or HC0001ZA, HC0002ZA...)
   prefix = 'BP', digit = 3, suffix = ''.
   prefix = 'HC', digit = 4, suffix = 'ZA'. respectively

Output:
1. Demographcis and imgaing related info as Demographics.csv
2. logging file as ExtractDicomInfo.log


Explanation:
Group (Base Class):
Base class for handling general folder and logging operations.
Manages folder identification, anonymized folder handling, and log writing.

Participant (Derived from Group):
Represents a specific participant.
Methods include get_participant_folders for retrieving DICOM folders, get_dcm_subtypes for identifying DICOM file types, and collect_dcm_info for extracting DICOM metadata.
Uses convert_to_dataframe to compile DICOM data into a structured DataFrame.

DataCollector (Standalone Class):
Manages the data collection process for multiple participants.
Uses get_all_participants to gather participant IDs and execute_collection to initiate and compile the process across participants.
Aggregates individual participant DataFrames into df_demographics and writes to CSV via write_to_csv.


     　　　　　　　　　　　　　　　　　　　　+-------------------------+
                                   |      Group (Base)       |
                                   +-------------------------+
                                   | - sourcedir: str        |
                                   +-------------------------+
                                   | + is_folder_empty()     |
                                   | + collect_subject_folders() |
                                   | + add_anonymized_subject_folders() |
                                   | + exclude_ineligible_folders() |
                                   | + write_to_log()        |
                                   +-------------------------+
                                              |
                                              |
                                  +------------------------+
                                  |   Participant (Child) |
                                  +------------------------+
                                  | - sub_prefix: str      |
                                  | - order: int           |
                                  | - order_digit: int     |
                                  | - sub_suffix: str      |
                                  | - general_id: str      |
                                  +------------------------+
                                  | + get_participant_folders() |
                                  | + get_dcm_subtypes()    |
                                  | + collect_dcm_info()    |
                                  | + convert_to_dataframe() |
                                  | + steps()               |
                                  +------------------------+
                                              |
                                              |
                         +---------------------------------------------+
                         |        DataCollector (Standalone)           |
                         +---------------------------------------------+
                         | - sourcedir: str                            |
                         | - sub_prefix: str                           |
                         | - order_digit: int                          |
                         | - sub_suffix: str                           |
                         | - df_demographics: DataFrame                |
                         +---------------------------------------------+
                         | + add_to_demographics()                     |
                         | + get_all_participants()                    |
                         | + extract_order()                           |
                         | + write_to_log()                            |
                         | + write_to_csv()                            |
                         | + execute_collection()                      |
                         +---------------------------------------------+

If you find a bug or bugs, please let me know.
yan.yuqi.psybio@gmail.com
