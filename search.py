import json
import pickle
from collections import defaultdict
from nltk.stem.porter import PorterStemmer
from sacremoses import MosesTokenizer
from pathlib import Path
import gradio as gr

'''
TO FIX: should not be saving and loading as pickle here, should do that in the indexer if anything, but even that maybe not good bc:
I need to read line by line because that's how I can find the correct token I'm looking for 
I can't hold the whole thing in memory in the final code
so that needs to be changed

also currently the code only does very simple boolean AND, nothing else
the rest needs to be implemented based on lectures

ALSO FIX THE INDEXER SO IT'S EASIER TO ASSOCIATE the A text WITH THE URL IT'S DESCRIBING
'''
stemmer = PorterStemmer()
tokenizer = MosesTokenizer()

def load_json_file(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            yield json.loads(line) #allows processing line by line the index, otherwise too big for memory

def create_inverted_index(file_path):
    inverted_index = {}
    for entry in load_json_file(file_path):
        token = entry['token']
        doc_ids = [doc_entry['doc_id'] for doc_entry in entry['entries']]
        if token in inverted_index:
            inverted_index[token].update(doc_ids)
        else:
            inverted_index[token] = set(doc_ids)
    return inverted_index

def tokenize_and_stem(text):
    tokens = tokenizer.tokenize(text)
    return [stemmer.stem(token) for token in tokens if token.isalnum()]

def boolean_and_query(inverted_index, query_text):
    query_terms = tokenize_and_stem(query_text)
    result_sets = [inverted_index.get(term, set()) for term in query_terms]
    return set.intersection(*result_sets) if result_sets else set()

# Paths
index_file_path = Path('final_index.json')
pickle_file_path = Path('inverted_index.pkl')

# Check if the pickled index exists and load/create accordingly
if pickle_file_path.exists():
    with open(pickle_file_path, 'rb') as f:
        inverted_index = pickle.load(f)
else:
    inverted_index = create_inverted_index(index_file_path)
    with open(pickle_file_path, 'wb') as f:
        pickle.dump(inverted_index, f)

def load_doc_id_map(file_path):
    with open(file_path, 'r') as file:
        url_to_doc_id = json.load(file)
    # Invert the mapping to get doc_id to URL WHOOPS
    return {str(doc_id): url for url, doc_id in url_to_doc_id.items()}

# Load the doc_id_map and invert it
doc_id_map_file_path = Path('doc_id_map.json')
doc_id_to_url = load_doc_id_map(doc_id_map_file_path)

def search(query):
    matching_docs = boolean_and_query(inverted_index, query)
    matching_urls = [doc_id_to_url[str(doc_id)] for doc_id in matching_docs]
    return '\n'.join(matching_urls)  # Returns URLs as a single string separated by new lines

# Create a Gradio interface
interface = gr.Interface(
    fn=search, 
    inputs="text", 
    outputs="text",
    title="Document Search Engine",
    description="Enter a query to search for documents.",
    allow_flagging='never'  # Disables the flagging option which basically just saves that output
)

# Run the interface
interface.launch(inbrowser=True)