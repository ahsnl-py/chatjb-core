from dataclasses import dataclass
import os
from typing import List
from service.db_service import DbService
from service.api_service import ApiService
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_community.vectorstores import MongoDBAtlasVectorSearch
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.chains import create_retrieval_chain
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain.retrievers import ContextualCompressionRetriever
from langchain.retrievers.document_compressors import LLMChainExtractor
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.llms import OpenAI
from dotenv import load_dotenv
import warnings

load_dotenv()

warnings.filterwarnings("ignore", category=UserWarning, module="langchain.chains.llm")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

@dataclass
class QueryResponse:
    query_text: str
    response_text: str
    sources: List[str]

class RAG():

    def __init__(self, dbservice:DbService, api:ApiService, isDebug=False) -> None:
        self.isDebug = isDebug
        self.db = dbservice
        self.web = api
        self.emb_model_openai = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY)

    def generate_embidding(self, text) -> List[float]:
        headers = {"Authorization": "Bearer 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"}
        return self.web.post(endpoint="", headers=headers, json={"inputs": text})

    def generate_embedding_openai(self, text) -> List[float]:
        return self.emb_model_openai.embed_query(text)

    def query_vector_mongo(self, query):
        llm = ChatOpenAI(openai_api_key=OPENAI_API_KEY, model="gpt-3.5-turbo", temperature=0)
        collection = self.db.get_collection("jobcz_embedded")
        vector_store = MongoDBAtlasVectorSearch( collection, self.emb_model_openai )
        docs = vector_store.max_marginal_relevance_search( query, k=1 )
        print(docs[0].page_content)

    def split_text_content(self, text, collectionName):

        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=0, length_function=len)
        texts = text_splitter.split_text(text=text)
        print(' Text split into: ' + str(len(texts)) + ' chunk')

        textSearch = MongoDBAtlasVectorSearch.from_texts(
            texts=texts,
            embedding=self.emb_model_openai,
            collection=self.db.get_collection(collectionName)) 
        
    def vector_query(self, query, collectionName) -> QueryResponse:
        llm = ChatOpenAI(openai_api_key=OPENAI_API_KEY, model="gpt-3.5-turbo", temperature=0)
        system_prompt = (
            "Use the given context to answer the question. "
            "By the end of each answer, get the Link to inform user more about the job info. "
            "If you don't know the answer, say you don't know. "
            "Use three sentence maximum and keep the answer concise.\n"
            "Context:\n{context}\n"
        )
        collection = self.db.get_collection(collectionName)
        vector_store = MongoDBAtlasVectorSearch( collection, self.emb_model_openai, index_name='vector_index' )
        retriever = vector_store.as_retriever()

        if self.isDebug:
            docs = vector_store.max_marginal_relevance_search(query=query, k=1)
            if len(docs) == 0:
                print("No relevant document found. Query complete")
                return
            else:
                print("Relevant document found: ")
                print("Document content: \n" + docs[0].page_content)

        prompt = ChatPromptTemplate.from_messages(
            [
                ("system", system_prompt),
                ("human", "{input}"),
            ]
        )
        question_answer_chain = create_stuff_documents_chain(llm, prompt)
        chain = create_retrieval_chain(retriever, question_answer_chain)
        res = chain.invoke({"input": query})
        return QueryResponse(
            query_text=query,
            response_text=res.get("answer", "No query return."),
            sources=res.get("page_content", [])
        )
        