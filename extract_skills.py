import pandas as pd
import sql.utils
from configparser import ConfigParser
import nltk
# nltk.download('punkt')
# nltk.download('averaged_perceptron_tagger')
# nltk.download('stopwords')
# nltk.download('wordnet')
# nltk.download('omw-1.4')
# nltk.download('words')
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from sklearn.feature_extraction.text import TfidfVectorizer

config = ConfigParser()
config.read('config.ini')

# setting db parameters
jobs_db = config.get('SQL', 'JOBS_DB')
title_descriptions_table = config.get('SQL', 'TITLE_DESCRIPTIONS_TABLE')
all_jobs_table = config.get('SQL', 'ALL_JOBS_TABLE')
skills_table = config.get('SQL', 'SKILLS_TABLE')
min_jobs = int(config.get('MODEL', 'MIN_NUM_JOBS_PER_TITLE'))

def get_df(conn, title_descriptions_table = title_descriptions_table, min_jobs = min_jobs):
    df = pd.read_sql_query("Select * from {0}".format(title_descriptions_table), conn)
    df = df[(df['num_occurences']>=min_jobs)]
    return df

def get_companies(conn, all_jobs_table = all_jobs_table):
    companies = pd.read_sql_query("Select distinct company_name from {0}".format(all_jobs_table), conn)
    company_tokens = []
    companies_list = companies['company_name'].tolist()
    for company in companies_list:
        company_tokens.extend(nltk.tokenize.word_tokenize(company.lower()))
    return company_tokens

def get_locations(conn, all_jobs_table = all_jobs_table):
    locations = pd.read_sql_query("Select distinct location from {0}".format(all_jobs_table), conn)
    location_tokens = []
    locations_list = locations['location'].tolist()
    for location in locations_list:
        location_tokens.extend(nltk.tokenize.word_tokenize(location.lower()))
    return location_tokens

def get_titles(df):
    titles = df['all_titles'].tolist()
    title_tokens = []
    for title in titles:
        title_tokens.extend(nltk.tokenize.word_tokenize(title.lower()))
    return title_tokens

def get_remove_tokens():
    with open("remove_tokens.txt") as f:
        remove_tokens = [line.rstrip('\n') for line in f]
    return remove_tokens


def preprocess_description(description, title_tokens, company_tokens, location_tokens, other_remove_tokens):
    tokens = nltk.tokenize.word_tokenize(description)
    words = set(nltk.corpus.words.words())
    word_tokens = [token.lower() for token in tokens if token.lower() in words
                    or token in ['.', ',']]

    token_tags = nltk.pos_tag(word_tokens)
    tags_to_include = ['.', 'JJ', 'JJR', 'JJS', 'NN', 'NNP', 'RB', 'RBR', 'RBS', 'MD', 'VB', 'VBD', 'VBG', 'VBN', 'VBP', 'VBZ']
    filtered_tokens = [token for token, tag in token_tags if tag in tags_to_include]

    # porter_stemmer = nltk.stem.PorterStemmer()
    # stemmed_tokens = [porter_stemmer.stem(token).lower() for token in filtered_tokens]

    lemmatizer = WordNetLemmatizer()
    lemmatized_tokens = [lemmatizer.lemmatize(token) for token in filtered_tokens]
    
    stop_words = stopwords.words('english')
    stop_words.extend(title_tokens)
    stop_words.extend(company_tokens)
    stop_words.extend(location_tokens)
    stop_words.extend(other_remove_tokens)
    stop_words_lemmatized = [lemmatizer.lemmatize(token) for token in stop_words]
    stop_words_removed = [token for token in lemmatized_tokens if token not in stop_words_lemmatized]

    string_description = " ".join(stop_words_removed)
    return string_description

def extract_skills(conn):
    df = get_df(conn = conn)

    company_tokens = get_companies(conn = conn)
    location_tokens = get_locations(conn = conn)
    title_tokens = get_titles(df = df)
    remove_tokens = get_remove_tokens()
    df['relevant_descriptions'] = df.apply(lambda x: preprocess_description(x['descriptions'], 
                                                    company_tokens = company_tokens,
                                                    location_tokens = location_tokens,
                                                    title_tokens= title_tokens,
                                                    other_remove_tokens = remove_tokens), axis=1)
    
    vectorizer = TfidfVectorizer(max_df = 0.65, min_df = 0.01, ngram_range=(1,2), strip_accents = "unicode")
    vectors = vectorizer.fit_transform(df['relevant_descriptions'].tolist())
    vectors_list = vectors.todense().tolist()
    skills_labels = vectorizer.get_feature_names()
    titles = df['all_titles'].tolist()
    skills_df = pd.DataFrame(vectors_list, columns = skills_labels, index = pd.Index(titles))

    top_skills_df = pd.DataFrame(skills_df.apply(lambda x: x.nlargest(10).index.tolist(), axis=1).tolist(), 
                           columns=['Top (1)','Top (2)','Top (3)','Top (4)','Top (5)',
                           'Top (6)','Top (7)','Top (8)','Top (9)','Top (10)'], index = pd.Index(titles))

    top_skills_vals = pd.DataFrame(skills_df.apply(lambda x: x.nlargest(10).tolist(), axis=1).tolist(), 
                           index = pd.Index(titles))
    top_skills_vals['confidence_level'] = top_skills_vals.sum(axis=1)
    top_skills_vals_conf_max = top_skills_vals['confidence_level'].max()
    top_skills_vals_conf_min = top_skills_vals['confidence_level'].min()
    top_skills_vals['confidence_level_normalized'] = (
        top_skills_vals['confidence_level']-top_skills_vals_conf_min)/(
            top_skills_vals_conf_max - top_skills_vals_conf_min)           
    
    top_skills_num_df = pd.merge(df[['all_titles','num_occurences']], 
                                    pd.merge(top_skills_df, top_skills_vals[['confidence_level_normalized']], 
                                    left_index = True, right_index = True),
                                    left_on = "all_titles", right_index = True)
    values_to_insert = list(top_skills_num_df.itertuples(index=False, name=None))
    return values_to_insert

def create_skills_table(conn, skills_table = skills_table):
    query_from_file = sql.utils.get_query("create_skills_table.sql")
    query_to_execute = query_from_file.format(skills_table)
    sql.utils.execute_query(conn = conn, query = query_to_execute)

def insert_skills_table(conn, values_to_insert = [], skills_table = skills_table):
    query_from_file = sql.utils.get_query("insert_skills_table.sql")
    query_to_execute = query_from_file.format(skills_table)
    sql.utils.executemany_query(conn = conn, query = query_to_execute, 
                            values = values_to_insert)

def drop_skills_table(conn, skills_table = skills_table):
    query_from_file = sql.utils.get_query("drop_table.sql")
    query_to_execute = query_from_file.format(skills_table)
    sql.utils.execute_query(conn = conn, query = query_to_execute)

def populate_skills_table(conn, values_to_insert = []):
    drop_skills_table(conn = conn)
    create_skills_table(conn = conn)
    insert_skills_table(conn = conn, values_to_insert = values_to_insert)
    conn.commit()

if __name__ == "__main__":
    conn = sql.utils.create_connection(jobs_db)
    top_skills = extract_skills(conn = conn)
    populate_skills_table(conn = conn, values_to_insert = top_skills)
    sql.utils.close_connection(conn)