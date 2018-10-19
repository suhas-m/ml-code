import logging
from pprint import pprint
from gensim.utils import simple_preprocess

import lib

EMAIL_REGEX = r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}"
#PHONE_REGEX = r"\(?(\d{3})?\)?[\s\.-]{0,2}?(\d{3})[\s\.-]{0,2}(\d{4})"
PHONE_REGEX = r"[6-9]\d{9}"

def candidate_name_extractor(input_string, nlp):
    #print(input_string)
    input_string = str(input_string)
    
    doc = nlp(input_string)
    
    # Extract entities
    doc_entities = doc.ents
    #print(doc_entities)
    # Subset to person type entities
    print("********* Entities")
    for v in doc_entities:
        print(v)
        newv = nlp(v.text.strip())
        print(newv.ents)
        for t in newv.ents:
            print("-----------")
            print(t)
            print(t.label_)
        print("***********")
        print(v.label_)
    doc_persons = filter(lambda x: x.label_ == 'PERSON', doc_entities)
    print("********* Persons")
    for k in doc_persons:
        print(k)
        print(k.text.strip().split())
    doc_persons = filter(lambda x: len(x.text.strip().split()) >= 2, doc_persons)
    print("********* Persons 2")
    for k in doc_persons:
        print(k)
    doc_persons = list(map(lambda x: x.text.strip(), doc_persons))
    print("********* Persons 3")
    print(doc_persons)
    # Assuming that the first Person entity with more than two tokens is the candidate's name
    
    if doc_persons:
        return doc_persons[0]
    return "NOT FOUND"


def extract_fields(df):
    for extractor, items_of_interest in lib.get_conf('extractors').items():
        df[extractor] = df['text'].apply(lambda x: extract_skills(x, extractor, items_of_interest))
    return df


def extract_skills(resume_text, extractor, items_of_interest):
    potential_skills_dict = dict()
    matched_skills = set()

    # TODO This skill input formatting could happen once per run, instead of once per observation.
    for skill_input in items_of_interest:

        # Format list inputs
        if type(skill_input) is list and len(skill_input) >= 1:
            potential_skills_dict[skill_input[0]] = skill_input

        # Format string inputs
        elif type(skill_input) is str:
            potential_skills_dict[skill_input] = [skill_input]
        else:
            logging.warn('Unknown skill listing type: {}. Please format as either a single string or a list of strings'
                         ''.format(skill_input))

    for (skill_name, skill_alias_list) in potential_skills_dict.items():

        skill_matches = 0
        # Iterate through aliases
        for skill_alias in skill_alias_list:
            # Add the number of matches for each alias
            skill_matches += lib.term_count(resume_text, skill_alias.lower())

        # If at least one alias is found, add skill name to set of skills
        if skill_matches > 0:
            matched_skills.add(skill_name)

    return matched_skills
