import logging
from pprint import pprint
from gensim.utils import simple_preprocess
import re

import lib

EMAIL_REGEX = r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}"
#PHONE_REGEX = r"\(?(\d{3})?\)?[\s\.-]{0,2}?(\d{3})[\s\.-]{0,2}(\d{4})"
PHONE_REGEX = r"[6-9]\d{9}"

def candidate_name_extractor(input_string, nlp, email):
    
    #print(input_string)
    input_string = str(input_string)
    
    doc = nlp(input_string)
    
    # Extract entities
    doc_entities = doc.ents
    print(doc_entities)
    # Subset to person type entities
    persons = []
    for v in doc_entities:
        print("----Entity---", v,"-----")
        print("----Label", v.label_)
        print(v.text.replace('\n', ''))
        newv = nlp(v.text.replace('\n', ''))
        for t in newv.ents:
           print("----New Entity -----", t, '---------')
           print("----New Label", t.label_)
           
           if (t.label_ == 'PERSON') :
               persons.append(t.text)
               
    print("Persons 1:", persons) 
   
    doc_persons = filter(lambda x: x.label_ == 'PERSON', doc_entities)
    
            
    #print("********* Persons")
    #print(doc_persons)
    #print("********End Persons")
    #for k in doc_persons:
    #    print(k)
    #    print(k.text.strip().split())
    doc_persons = filter(lambda x: len(x.text.strip().split()) >= 2, doc_persons)
    #print("********* Persons 2")
    doc_persons = list(map(lambda x: x.text.strip(), doc_persons))
    persons = persons + list(set(doc_persons) - set(persons))
    print("Persons 2:", persons)    
    print("Email:", email)
    person_dict = {}
    
    for person in persons:
        person_terms = person.split()
        for term in person_terms:
            #print("Lower case term:",  term.lower(), "email string:", email.astype('|S'))
            if term.lower() in email.to_string() :
                if person in person_dict.keys():
                    person_dict[person] = person_dict[person]+1
                else :
                    person_dict[person] = 1
    
    print("Persons Dictionary:", person_dict)            
    # Assuming that the first Person entity with more than two tokens is the candidate's name
    previous_count = 0
    previous_person = None
    for key in person_dict:
        if ((key != previous_person) and (previous_count < person_dict[key])) :
            previous_count = person_dict[key];
            previous_person = key
            
    if previous_person:
        return previous_person
    else :
        if (persons) :
            return persons[0]
        
    return "NOT_FOUND"
    
"""
def candidate_name_extractor(input_string, nlp):
    #input_string = unicode(input_string)

    doc = nlp(input_string)

    # Extract entities
    doc_entities = doc.ents

    # Subset to person type entities
    doc_persons = filter(lambda x: x.label_ == 'PERSON', doc_entities)
    doc_persons = filter(lambda x: len(x.text.strip().split()) >= 2, doc_persons)
    doc_persons =list( map(lambda x: x.text.strip(), doc_persons))

    # Assuming that the first Person entity with more than two tokens is the candidate's name
    if doc_persons:
        return doc_persons[0]
    return "NOT FOUND"
"""
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
