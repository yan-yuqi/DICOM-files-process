import os
import re
import pandas as pd
import pydicom
from collections import defaultdict
import logging


class Group:
    def __init__(self, sourcedir: str):
        if os.path.exists(sourcedir):
            self.sourcedir = sourcedir
        else:
            raise ValueError('Source directory does not exist.')

    @staticmethod
    def is_folder_empty(folder_path: str) -> bool:
        """
        Whether the folder is empty or not. Situation on Mac, in particular, about DS.store is considered.
        """
        # Verify path
        if not os.path.isdir(folder_path):
            raise NotADirectoryError(folder_path)

        # List system files ON MAC.
        ignore_files = ['.DS_Store', '._', '.Spotlight-V100', '.Trashes', '.TemporaryItems', '.fseventsd']
        files = os.listdir(folder_path)
        files = [f for f in files if f not in ignore_files]
        for f in files:
            if f.endswith('.DCM'):
                return False
        else:
            return True

    def collect_subject_folders(self, participant: str) -> tuple:
        """
        To collect all folders which contain Dicom files related to particular participant. Because there are usually
        multiple Dicom files related to particular participant, and they are possible in different layer folders.
        Example layer folders given as following. Wherein, '192.dcm' stand for 192 dcm files exist in upper folder.

        file/
            sub-012/
                000000001/
                    192.dcm
                000000002/
                    9760.dcm
            sub-013/
                208.dcm
                sub-013_001/
                    76.dcm
                sub-013_002/
                    38.dcm
                sub-013_003/
                    244.dcm
            sub-014_001/
                208.dcm
            sub-014_002/
                76.dcm
            sub-014_003/
                244.dcm
            sub-015_001/
                208.dcm
            sub-015_002/
                244.dcom
            sub-015_001(1)/
                208.dcm
           sub-015_002(2)/
                244.dcm

        :param participant: The general ID of participant.
        :return: A tuple of folders' path that related to particular participant.
                 e.g. (('sub-013', 'file/sub-013'), ('sub-013_001', 'file/sub-013/sub-013_001'),
                     ('sub-013_002', 'file/sub-013/sub-013_002'),('sub-013_003', 'file/sub-013/sub-013_003'))
        """
        # Record the folder name and the path.
        first_participant_folders = []

        # Scanning all the directories and files
        for root, dirs, files in os.walk(self.sourcedir):
            for d in dirs:
                # To find out the folders which related to particular participant
                if participant in d:
                    first_participant_folders.append((d, os.path.join(root, d)))

        return tuple(first_participant_folders)

    @staticmethod
    def add_anonymized_subject_folders(participant_folders: tuple) -> tuple:
        """
        Add folders those names do not contain participant general ID but work as other subject related folders.
        Notice that new added folder or folders may be duplicated.
        :param participant_folders: tuple of (folder, path).
        :return: A tuple of folders' path that related to particular participant, anonymized folders added.
        """
        # Record folder name and the path.
        second_participant_folders = []

        # Scanning all the folders and add anonymized subject folders into list.
        for folder, path in participant_folders:
            for root, dirs, files in os.walk(path):
                # If directories exist in present working directory.
                if dirs:
                    for d in dirs:
                        second_participant_folders.append((d, os.path.join(root, d)))

        # Combine original and new added folders
        second_participant_folders = list(participant_folders) + second_participant_folders

        return tuple(second_participant_folders)

    def exclude_ineligible_folders(self, participant_folders: tuple) -> tuple:
        """
        Remove the duplicated and empty folders in participant related folders.
        :param participant_folders: tuple of (folder, path).
        :return: participant_folders after excluded folders.
        """
        # Record eligible folder
        third_participant_folders = []

        # Change tuple record to list
        tmp_folders = list(participant_folders)

        # Exclude duplicated folders
        tmp_folders = list(set(tmp_folders))

        # Exclude empty folders
        for folder, path in tmp_folders:
            if not self.is_folder_empty(path):
                third_participant_folders.append((folder, path))

        return tuple(third_participant_folders)

    @staticmethod
    def write_to_log(absolute_path: str, message: str, level='info') -> None:
        """
        Write the content to a log file. There are three levels: 'info', 'warning' and 'error'.
        Normal information is labeled as 'info', information may cause failure is labeled as 'warning', whereas
        information about error is labeled as 'error'.
        :param absolute_path: absolute path of sourcedir.
        :param message: The content to write.
        :param level: The log level.
        """
        # Log setting
        logging.basicConfig(
            filename='{}/{}.log'.format(absolute_path, 'ExtractDicomInfo'),
            filemode='a',
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s',
        )

        # Output
        if level == 'info':
            logging.info(message)
        elif level == 'warning':
            logging.warning(message)
        elif level == 'error':
            logging.error(message)
        else:
            raise ValueError('Invalid log level: {}'.format(level))


class Participant(Group):
    """
    To create Participant instance. The General ID consist of prefix(str), order(int) and suffix (in where, use '' to
    express the missing prefix or suffix).
    """
    def __init__(self, sourcedir: str, sub_prefix: str, order: int, order_digit: int,
                 sub_suffix: str, outputdir=None):
        super().__init__(sourcedir)
        # Verify and assign path to variable
        if os.path.exists(sourcedir):
            self.sourcedir = sourcedir
        else:
            raise ValueError('Source directory does not exist.')
        if not outputdir:
            self.outputdir = self.sourcedir
        elif outputdir and os.path.exists(outputdir):
            self.outputdir = outputdir

        self.order = order
        if len(str(self.order)) > order_digit:
            raise ValueError('Order digit must be less than or equal to order digit')

        self.general_id = sub_prefix + str(order).zfill(order_digit) + sub_suffix

        self.participant_folders = None
        self.participant_subtypes = None
        self.participant_info = None
        self.participant_dataframe = None

    def get_participant_folders(self) -> tuple:
        """
        To collect all folders which contain Dicom files related to particular participant.
        :return: A tuple of all participant folders.
        """
        first_participant_folders = self.collect_subject_folders(self.general_id)

        # -------------------------------------Test-Block-Start-------------------------------------------------
        # print("\nFirst:")
        # for t in first_participant_folders:
        #     print(t)
        # print(len(first_participant_folders))
        # -------------------------------------Test-Block-End---------------------------------------------------

        second_participant_folders = self.add_anonymized_subject_folders(first_participant_folders)

        # -------------------------------------Test-Block-Start-------------------------------------------------
        # print("\nSecond:")
        # for t in second_participant_folders:
        #     print(t)
        # print(len(second_participant_folders))
        # -------------------------------------Test-Block-End---------------------------------------------------

        third_participant_folders = self.exclude_ineligible_folders(second_participant_folders)

        # -------------------------------------Test-Block-Start-------------------------------------------------
        # print("\nThird:")
        # for t in third_participant_folders:
        #     print(t)
        # print(len(third_participant_folders))
        # -------------------------------------Test-Block-End---------------------------------------------------

        participant_folders = sorted(third_participant_folders, key=lambda x: x[0])

        return tuple(participant_folders)

    def get_dcm_subtypes(self, participant_folders: tuple) -> tuple:
        """
        To count dicom files by subtype in each of participant folders.
        :param participant_folders: tuple of (folder, path).
        :return: participant_subtype: tuple of (subtype, path of typical dicom file).

                 e.g.
                    'sub-056'               ,  'file/sub-056/00000001.dcm'   , 208  # only one type in this folder
                    'sub-056_001-1(233size)', 'file/sub-056_001/00000001.dcm', 244  # Two types in this folder, type1
                    'sub-056_001-2(343size)', 'file/sub-056_001/00000002.dcm', 244  # Two types in this folder, type2

        """
        participant_subtypes = []

        # Size threshold of same key cluster
        size_threshold = 3

        for folder, path in participant_folders:
            # Size group
            dcm_count = defaultdict(list)

            # Get all the dicom file in the folder
            files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and f.endswith('.DCM')]

            # Exception process
            if not files:
                self.write_to_log(
                    absolute_path=self.outputdir,
                    message='No dicom files found in folder {} ({})'.format(folder, path),
                    level='error'
                    )
                continue

            # Count different type of dicom files
            for file in files:
                size_kb = os.path.getsize(os.path.join(path, file)) / 1024
                for existing_size in dcm_count.keys():
                    if abs(size_kb - float(existing_size)) <= size_threshold:
                        dcm_count[str(int(existing_size))].append(file)
                        break
                else:
                    dcm_count[str(int(size_kb))].append(file)

            # ---------------DEBUG--------------------
            # print(f"{self.general_id}-{folder}: ")
            # for key, value in dcm_count.items():
            #     print(f'{key}: {value}')
            # ---------------E-N-D--------------------

            # Reclassification
            if len(dcm_count) == 1:
                typical_dcm = list(dcm_count.values())[0][0]
                participant_subtypes.append(
                    (folder,
                     os.path.join(path, typical_dcm),
                     sum(len(files) for files in dcm_count.values()))
                )

            elif len(dcm_count) >= 2:
                for i, (key, value) in enumerate(dcm_count.items()):
                    typical_dcm = value[0]
                    participant_subtypes.append(
                        ("{}-{}({}size)".format(folder, i, key),
                         os.path.join(path, typical_dcm),
                         len(value))
                    )

        return tuple(participant_subtypes)

    def collect_dcm_info(self, participant_subtypes: tuple) -> tuple:
        """To collect participant information from dicom file."""
        dcm_info = []

        # Read dicom file by folder
        for folder, path, file_count in participant_subtypes:
            try:
                # Read dicom file
                try:
                    dicom_data = pydicom.dcmread(path)
                except FileNotFoundError as e:
                    self.write_to_log(
                        absolute_path=self.outputdir,
                        message='Dicom files reading fails {} as {}'.format(path, e),
                        level='warning'
                    )
                    continue

                # Get demographic info from dicom file
                sequence_name = dicom_data.get('SequenceName', 'Unknown')
                image_type = dicom_data.get('ImageType', 'Unknown')
                procedure_step_start_date = dicom_data.get('PerformedProcedureStepStartDate', 'Unknown')
                procedure_step_start_time = dicom_data.get('PerformedProcedureStepStartTime', 'Unknown')
                manufacturer = dicom_data.get('Manufacturer', 'Unknown')
                patient_name = dicom_data.get('PatientName', 'Unknown')
                patient_id = dicom_data.get('PatientID', 'Unknown')
                patient_sex = dicom_data.get('PatientSex', 'Unknown')
                patient_age = dicom_data.get('PatientAge', 'Unknown')
                patient_size = dicom_data.get('PatientSize', 'Unknown')
                patient_weight = dicom_data.get('PatientWeight', 'Unknown')
                echo_time = dicom_data.get('EchoTime', 'Unknown')
                dcm_info.append((folder, (file_count, patient_sex, patient_age, patient_size, patient_weight,
                                          sequence_name, procedure_step_start_date, procedure_step_start_time,
                                          patient_name, patient_id, echo_time, image_type, manufacturer,)))
            except FileNotFoundError:
                print('Error happened when try to read dicom file in folder {}.'.format(folder))
                continue

        return tuple(dcm_info)

    def convert_to_dataframe(self, dcm_info: tuple) -> pd.DataFrame:
        """
        Add information, which collected by collect_dcm_files and stored in dcm_info, to a new dataframe.
        This aims to create 2-level hierarchical dataframe as following.
        e.g.

        Participant Folder    1st 2nd 3rd...
        --------------------------------------
        sub056    sub056-1    12  13  14 ...
                  sub056-2    22  23  24 ...
                  sub056-3    32  33  34 ...
        sub057    sub057-001  42  43  44 ...
                  sub057-002  52  53  54 ...
                  sub057-003  62  63  64 ...

        :param dcm_info: Dicom information of a particular imaging method.
        :return: A dataframe which contains dcm_info
        """

        # Create multi-index for dataframe
        index_tuple = [(self.general_id, method_folder) for method_folder, _ in dcm_info]
        multi_index = pd.MultiIndex.from_tuples(index_tuple)

        # Create column for dataframe
        columns = ['FileCount', 'Sex', 'Age', 'Height', 'Weight', 'SequenceName', 'Date', 'Time', 'Name',
                   'PatientID', 'EchoTime', 'ImageType', 'Manufacturer']

        # Create data for dataframe
        participant_data = [list(data) for _, data in dcm_info]

        # Add data to dataframe
        df_participant = pd.DataFrame(participant_data, index=multi_index, columns=columns)

        return df_participant

    def steps(self) -> None:

        # Participant folders
        self.write_to_log(
            self.sourcedir,
            '{}: Participant folders collecting starts...'.format(self.general_id),
            level='info'
        )
        self.participant_folders = self.get_participant_folders()
        self.write_to_log(
            self.sourcedir,
            '{}: Participant folders collecting finished.'.format(self.general_id),
            level='info'
        )

        # Participant subtypes
        self.write_to_log(
            self.sourcedir,
            '{}: Participant subtype getting starts...'.format(self.general_id),
            level='info'
        )
        self.participant_subtypes = self.get_dcm_subtypes(self.participant_folders)
        self.write_to_log(
            self.sourcedir,
            '{}: Participant subtype getting finished.'.format(self.general_id),
            level='info'
        )

        # Collect Dicom information
        self.write_to_log(
            self.sourcedir,
            '{}: Participant Dicom information collecting starts...'.format(self.general_id),
            level='info'
        )
        self.participant_info = self.collect_dcm_info(self.participant_subtypes)
        self.write_to_log(
            self.sourcedir,
            '{}: Participant Dicom information collecting finished.'.format(self.general_id),
            level='info'
        )

        # Convert to dataframe
        self.write_to_log(
            self.sourcedir,
            '{}: Converting to dataframe starts...'.format(self.general_id),
            level='info'
        )
        self.participant_dataframe = self.convert_to_dataframe(self.participant_info)
        self.write_to_log(
            self.sourcedir,
            '{}: Converting to dataframe finished.'.format(self.general_id),
            level='info'
        )


class DataCollector:
    def __init__(self, sourcedir: str, sub_prefix: str, order_digit: int, sub_suffix: str,
                 outputdir=None):
        # Verify and assign path to variable
        if os.path.exists(sourcedir):
            self.sourcedir = sourcedir
        else:
            raise ValueError('Source directory does not exist.')
        if not outputdir:
            self.outputdir = self.sourcedir
        elif outputdir and os.path.exists(outputdir):
            self.outputdir = outputdir

        self.sub_prefix = sub_prefix
        self.sub_suffix = sub_suffix
        self.order_digit = order_digit

        self.participant_ids = None
        self.df_demographics = pd.DataFrame()

        self.participant_number = 0

    def add_to_demographics(self, personal_demographics: pd.DataFrame) -> None:
        """
        Add demographics to df_demographics dynamically. If df_demographics is empty, add new info as df_demographics.
        :param personal_demographics:
        :return: df_demographics with new added participant info
        """
        if self.df_demographics.empty:
            self.df_demographics = personal_demographics.copy()
        else:
            self.df_demographics = pd.concat([self.df_demographics, personal_demographics])

    def get_all_participants(self) -> tuple:
        """Get all participants' General ID by matching the pattern which created by using regular expression."""
        # Regular expression pattern
        pattern = rf'{re.escape(self.sub_prefix)}\d{{{self.order_digit}}}{re.escape(self.sub_suffix)}'

        # Extract participants by scanning all folders hierarchically
        participant_ids = []

        for root, dirs, files in os.walk(self.sourcedir):
            if not dirs:
                pass
            else:
                for folder_name in dirs:
                    match = re.search(pattern, folder_name)
                    if match:
                        participant_ids.append(match.group(0))

        # Exclude duplicated participants
        participant_ids = list(set(participant_ids))

        # Sorting
        participant_ids = sorted(participant_ids)

        return tuple(participant_ids)

    def extract_order(self, participant_id: str) -> int:
        """
        Extract participants Order Number.
        :param participant_id: Participant General ID.
        :return: Participant Order Number.
        """
        # Regular expression pattern
        pattern = rf'{re.escape(self.sub_prefix)}(\d{{{self.order_digit}}}){re.escape(self.sub_suffix)}'

        # Extraction
        match = re.search(pattern, participant_id)
        if match:
            order = int(match.group(1))
        else:
            self.write_to_log(
                message=f'{participant_id}: Order could not be extracted.',
                level='error'
            )
            raise ValueError(f'Order could not be extracted from {participant_id}.')

        return order

    def write_to_log(self, message: str, level='info') -> None:
        """
        Write the content to a log file. There are three levels: 'info', 'warning' and 'error'.
        Normal information is labeled as 'info', information may cause failure is labeled as 'warning', whereas
        information about error is labeled as 'error'.
        :param message: The content to write.
        :param level: The log level.
        :return: None
        """
        # Log setting
        logging.basicConfig(
            filename='{}/{}.log'.format(self.outputdir, 'ExtractDicomInfo'),
            filemode='a',
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s',
        )

        # Output
        if level == 'info':
            logging.info(message)
        elif level == 'warning':
            logging.warning(message)
        elif level == 'error':
            logging.error(message)
        else:
            raise ValueError('Invalid log level: {}'.format(level))

    def write_to_csv(self) -> None:
        """Write all demographics data to csv"""
        if not self.df_demographics.empty:
            output_path = os.path.join(self.outputdir, 'Demographics.csv')
            self.df_demographics.to_csv(output_path)
        else:
            self.write_to_log(
                message=f'Demographics dataframe is blank.',
                level='error'
            )
            raise ValueError(f'Demographics dataframe is blank.')

    def execute_collection(self) -> None:
        """ Data collection execution"""
        # Get all participants
        self.participant_ids = self.get_all_participants()

        # Execution for each participant_ids
        for participant_id in self.participant_ids:
            participant_dataframe = None
            try:
                # Create participant instance.
                order = self.extract_order(participant_id)
                participant = Participant(
                    sourcedir=self.sourcedir,
                    sub_prefix=self.sub_prefix,
                    order=order,
                    order_digit=self.order_digit,
                    sub_suffix=self.sub_suffix,
                    outputdir=self.outputdir
                )

                # Intra-participant processing
                participant.steps()
                participant_dataframe = participant.participant_dataframe
            except ValueError as e:
                self.write_to_log(
                    message=f'{participant_id}: ValueError for participant: {e}',
                    level='warning'
                )

            except FileNotFoundError as e:
                self.write_to_log(
                    message=f'{participant_id}: FileNotFoundError for participant: {e}',
                    level='warning'
                )

            except TypeError as e:
                self.write_to_log(
                    message=f'{participant_id}: TypeError for participant: {e}',
                    level='warning'
                )

            except Exception as e:
                self.write_to_log(
                    message=f'{participant_id}: Unexpected error for participant: {e}',
                    level='warning'
                )

            # Add to demographics
            if not participant_dataframe.empty:
                self.add_to_demographics(participant_dataframe)
                self.write_to_log(
                    message='{}: Participant processes completed successfully.'.format(participant_id),
                    level='info'
                )
            else:
                self.write_to_log(
                    message=f'{participant_id}: Data collection finished unsuccessfully',
                    level='error'
                )

        # Output
        self.write_to_csv()


# -------------------------------Parameters-------------------------------------------------

source_dir = '/Users/UserName/Desktop/Proj/'
prefix = 'HC'
digit = 3
suffix = ''

# -------------------------------Execution--=------------------------------------------------
data_collector = DataCollector(source_dir, prefix, digit, suffix)
data_collector.execute_collection()
