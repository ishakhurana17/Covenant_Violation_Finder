import pandas as pd
import datetime as dt
import os
from collections import defaultdict
import shutil
import glob
from secedgar.filings import Filing, FilingType
import random
from wordcloud import WordCloud


class CovenantViolationFinder:
    def __init__(self):
        self.execute()

    @staticmethod
    def __get_file_information(fp, line):
        if 'FILED AS OF DATE:' in line:
            date = line.split(':')[1].strip()
            return ('date', date)
        elif 'COMPANY CONFORMED NAME:' in line:
            company_name = line.split(':')[1].strip()
            return ('company_name', company_name)
        elif 'BUSINESS ADDRESS:' in line:
            line = fp.readline()
            address = ''
            cnt = 12
            while line.strip() != 'MAIL ADDRESS:' and cnt > 0:
                address = f'{address}{line.strip()}\n'
                line = fp.readline()
                cnt -= 1
            return ('address', address)
        elif 'FORM TYPE:' in line:
            form_type = line.split(':')[1].strip()
            return ('form_type', form_type)
        else:
            return None

    def __get_violations_for_file(self, file_path):
        target_words = ['covenant', 'waiv', 'viol', 'in default', 'modif', 'not in compliance']
        map_targets = defaultdict(set)
        local_word_count = dict()
        for word in target_words:
            local_word_count[word] = 0
        with open(file_path, encoding="utf-8") as fp:
            line = fp.readline()
            cnt = 1
            while line:
                found_targets = [target_word for target_word in target_words if target_word in line]
                if found_targets:
                    [map_targets[t].add(cnt) for t in found_targets]
                line = fp.readline()
                cnt += 1
        for word in map_targets:
            local_word_count[word] = len(map_targets[word])

        if 'covenant' not in map_targets:
            return 0, local_word_count

        total_violations = defaultdict(list)
        non_covenant_target_words = list(map_targets.keys())
        non_covenant_target_words.remove('covenant')

        for word in non_covenant_target_words:
            # Create a set to store all valid lines where non_covenant_target word can exist
            lines_to_check = self.__get_lines_where_covenant_can_exist(map_targets[word])
            for line in map_targets['covenant']:
                if line in lines_to_check:
                    total_violations[line].append(word)
        return len(total_violations), local_word_count

    @staticmethod
    def __get_lines_where_covenant_can_exist(exact_lines_containing_target_word):
        lines_to_check = set()
        for line in exact_lines_containing_target_word:
            for i in range(line-3, line+3):
                if i > 0:
                    lines_to_check.add(i)
        return lines_to_check

    def __get_file_metadata(self, file_path):
        file_meta = dict()
        with open(file_path, encoding="utf-8") as fp:
            line = fp.readline()
            # Check first 100 lines
            cnt = 100
            while cnt > 0:
                info = self.__get_file_information(fp, line)
                if info:
                    file_meta[info[0]] = info[1]
                if all(metadata in file_meta for metadata in ['date', 'company_name', 'address', 'form_type']):
                    break
                line = fp.readline()
                cnt -= 1
        if not all(metadata in file_meta for metadata in ['date', 'company_name', 'address', 'form_type']):
            raise Exception(f'File={file_path} does not contain required metadata information')

        date = dt.datetime.strptime(file_meta['date'], '%Y%m%d').date()
        file_meta['year'] = date.year
        file_meta['quarter'] = f'Q{(int(date.month)-1)//3 + 1}'
        token = file_meta['address'].strip().split('\n')
        for i in range(0, len(token)):
            tmp = token[i].strip()
            if tmp.startswith('ZIP:'):
                file_meta['zip'] = tmp.split(':')[1].strip()
                break
        return file_meta

    def __get_data(self, cik, filing_type, data_set):
        result = pd.DataFrame()
        filing_word_count = dict()
        my_filings = Filing(cik=str(cik), filing_type=filing_type)
        path = f'../data/company_filings/{cik}_{filing_type.value}/'
        if not os.path.exists(path):
            try:
                print(f'Fetching data for cik={cik}, filing_type={filing_type}')
                my_filings.save(path)
            except:
                try:
                    if os.path.exists(path):
                        shutil.rmtree(path)
                except OSError as e:
                    print("Error: %s : %s" % (path, e.strerror))
        else:
            print(f'Skipping data fetching. Using cache at {path}')
        for subdir, dirs, files in os.walk(path):
            for file in files:
                file_metadata = self.__get_file_metadata(f'{subdir}/{file}')
                for url in my_filings.get_urls():
                    if url.rsplit('/')[-1].strip() == file:
                        file_metadata['url'] = url
                        break
                assert len(file_metadata) == 8, "Could not get all relevant metadata: %r" % file_metadata
                if file_metadata['year'] < 2007 or \
                        (file_metadata['form_type'] != '10-K' and file_metadata['form_type'] != '10-Q'):
                    print(f'Skipping file. year={file_metadata["year"]} form_type={file_metadata["form_type"]}')
                    continue
                violations_in_file, local_word_count = self.__get_violations_for_file(f'{subdir}/{file}')

                file_info = {'cik': cik,
                             'firm name': file_metadata['company_name'],
                             'firm address': file_metadata['address'],
                             'zip code': str(file_metadata['zip']),
                             'year': file_metadata['year'],
                             'quarter': file_metadata['quarter'] if filing_type is FilingType.FILING_10Q else None,
                             'url': file_metadata['url'],
                             'filing type': filing_type.value,
                             'dataset': data_set,
                             'has covenant violation': 0 if violations_in_file == 0 else 1,
                             'total violations': violations_in_file
                             }
                result = result.append(pd.DataFrame(file_info, index=[0]))
                for word in local_word_count:
                    if word in filing_word_count:
                        filing_word_count[word] = filing_word_count[word] + local_word_count[word]
                    else:
                        filing_word_count[word] = local_word_count[word]
        return result, filing_word_count

    def execute(self):
        # Read file
        df_sec_covenant_violations = pd.read_excel('./../data/sec_covenant_violations_24_Sep_2012.xlsx')
        # Drop NA values
        df_sec_covenant_violations = df_sec_covenant_violations.dropna()
        df_sec_covenant_violations['date'] = pd.to_datetime(df_sec_covenant_violations['date'])
        # Find rows where year is from 2007 to present
        is_2007_to_present = df_sec_covenant_violations['date'] >= dt.datetime.strptime('Jan 1 2007  12:00AM',
                                                                                        '%b %d %Y %I:%M%p')
        # Filter rows by given year condition
        df_sec_covenant_violations = df_sec_covenant_violations[is_2007_to_present]

        # is_10_K = df_sec_covenant_violations['formtype'] == '10-K'
        is_10_K = list(df_sec_covenant_violations['formtype'] == '10-K')
        is_10_Q = list(df_sec_covenant_violations['formtype'] == '10-Q')
        is_10_K_or_10_Q = []
        for i in range(len(is_10_K)):
            is_10_K_or_10_Q.append(is_10_K[i] or is_10_Q[i])
        df_sec_covenant_violations = df_sec_covenant_violations[is_10_K_or_10_Q]

        # Drop duplicates
        df_sec_covenant_violations.drop_duplicates(['cik'], keep='last')

        # Dow Jones ciks
        dow_jones_industrial_avg = ['4962','66740', '4962', '320193', '12927', '18230', '93410', '858877', '21344',
                                    '1744489', '1666700', '34088', '40545', '886982', '354950', '50863', '51143',
                                    '200406', '19617', '63908', '310158', '789019', '320187', '78003', '80424',
                                    '86312', '731766', '732712', '1403161', '104169', '1618921']

        # Remove all dow jones firms
        df_sec_covenant_violations[~df_sec_covenant_violations['cik'].isin(dow_jones_industrial_avg)]

        cik_covenant_violations = []
        for i in range(0, 70):
            cik_covenant_violations.append(random.choice(list(df_sec_covenant_violations['cik'])))

        result_column_names = ['cik', 'firm name', 'firm address', 'zip code', 'year', 'quarter', 'url',
                               'filing type', 'dataset', 'has covenant violation', 'total violations']
        df_result = pd.DataFrame(columns=result_column_names)

        data_sets = [('Dow Jones Industrial Avg', dow_jones_industrial_avg),
                     ('Sec Covenant Violation', cik_covenant_violations)]
        filing_types = [FilingType.FILING_10K, FilingType.FILING_10Q]
        total_word_count = dict()

        for data_set in data_sets:
            print(f'*******Processing DataSet={data_set[0]}*******')
            for i in range(0, len(data_set[1])):
                for filing_type in filing_types:
                    cik = data_set[1][i]
                    print(f'{i+1}: Processing cik={cik} Filing_Type={filing_type.value}')
                    try:
                        result, filing_word_count = self.__get_data(cik, filing_type, data_set[0])
                    except Exception as e:
                        print(f'Error occurred! Skipping cik={cik} Filing_Type={filing_type.value}\n{e}')
                        continue
                    df_result = df_result.append(result)
                    for word in filing_word_count:
                        if word in total_word_count:
                            total_word_count[word] = total_word_count[word] + filing_word_count[word]
                        else:
                            total_word_count[word] = filing_word_count[word]
                df_result.to_excel(f'./../result/result_{i+1}.xls', index=False)
            print(f'*******Processing DataSet={data_set[0]} Finished*******')

        df_result.to_excel('./../result/result.xls', index=False)
        # Get word cloud
        cloud = WordCloud(background_color="white").generate_from_frequencies(total_word_count)
        cloud.to_file('./../result/word-cloud.png')

        # Get list of all intermediate results
        intermediate_results = glob.glob('./../result/result_*.xls')
        for intermediate_result in intermediate_results:
            try:
                os.remove(intermediate_result)
            except:
                print("Error while deleting file : ", intermediate_result)


if __name__ == "__main__":
    CovenantViolationFinder()
