import streamlit as st
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime
import json
import re
import time
from urllib.parse import urljoin, urlparse
import sqlite3
import hashlib
from typing import List, Dict, Tuple
import openai
from sentence_transformers import SentenceTransformer
import faiss
import pickle
import os
from dataclasses import dataclass
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DocumentChunk:
    """Represents a chunk of documentation with metadata"""
    content: str
    url: str
    title: str
    section: str
    chunk_id: str
    timestamp: datetime

class CelonisDocScraper:
    """Scrapes Celonis documentation and community content"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
# Key Celonis documentation URLs
        # Key Celonis documentation URLs
        self.doc_urls = {
            'getting_started_celonis_platform': 'https://docs.celonis.com/en/getting-started-with-the-celonis-platform.html',
            'managing_user_profile': 'https://docs.celonis.com/en/managing-your-user-profile.html',
            'troubleshooting_access_celonis_platform': 'https://docs.celonis.com/en/troubleshooting--access-to-your-celonis-platform.html',
            'release_notes': 'https://docs.celonis.com/en/release-notes.html',
            'feature_release_types': 'https://docs.celonis.com/en/feature_release_types.html',
            'private_public_preview': 'https://docs.celonis.com/en/private-public-preview.html',
            'planned_releases': 'https://docs.celonis.com/en/planned-releases.html',
            'release_notes_1728433': 'https://docs.celonis.com/en/release-notes-1728433.html',
            'june_2025_release_notes': 'https://docs.celonis.com/en/june-2025-release-notes.html',
            'may_2025_release_notes': 'https://docs.celonis.com/en/may-2025-release-notes.html',
            'april_2025_release_notes': 'https://docs.celonis.com/en/april-2025-release-notes.html',
            'march_2025_release_notes': 'https://docs.celonis.com/en/march-2025-release-notes.html',
            'february_2025_release_notes': 'https://docs.celonis.com/en/february-2025-release-notes.html',
            'january_2025_release_notes': 'https://docs.celonis.com/en/january-2025-release-notes.html',
            'release_notes_2024': 'https://docs.celonis.com/en/release-notes-2024.html',
            'december_2024_release_notes': 'https://docs.celonis.com/en/december-2024-release-notes.html',
            'november_2024_release_notes': 'https://docs.celonis.com/en/november-2024-release-notes.html',
            'october_2024_release_notes': 'https://docs.celonis.com/en/october-2024-release-notes.html',
            'september_2024_release_notes': 'https://docs.celonis.com/en/september-2024-release-notes.html',
            'august_2024_release_notes': 'https://docs.celonis.com/en/august-2024-release-notes.html',
            'july_2024_release_notes': 'https://docs.celonis.com/en/july-2024-release-notes.html',
            'june_2024_release_notes': 'https://docs.celonis.com/en/june-2024-release-notes.html',
            'may_2024_release_notes': 'https://docs.celonis.com/en/may-2024-release-notes.html',
            'april_2024_release_notes': 'https://docs.celonis.com/en/april-2024-release-notes.html',
            'march_2024_release_notes': 'https://docs.celonis.com/en/march-2024-release-notes.html',
            'february_2024_release_notes': 'https://docs.celonis.com/en/february-2024-release-notes.html',
            'january_2024_release_notes': 'https://docs.celonis.com/en/january-2024-release-notes.html',
            'release_notes_2023': 'https://docs.celonis.com/en/release-notes-2023.html',
            'december_2023_release_notes': 'https://docs.celonis.com/en/december-2023-release-notes.html',
            'november_2023_release_notes': 'https://docs.celonis.com/en/november-2023-release-notes.html',
            'october_2023_release_notes': 'https://docs.celonis.com/en/october-2023-release-notes.html',
            'september_2023_release_notes': 'https://docs.celonis.com/en/september-2023-release-notes.html',
            'august_2023_release_notes': 'https://docs.celonis.com/en/august-2023-release-notes.html',
            'july_2023_release_notes': 'https://docs.celonis.com/en/july-2023-release-notes.html',
            'june_2023_release_notes': 'https://docs.celonis.com/en/june-2023-release-notes.html',
            'may_2023_release_notes': 'https://docs.celonis.com/en/may-2023-release-notes.html',
            'april_2023_release_notes': 'https://docs.celonis.com/en/april-2023-release-notes.html',
            'march_2023_release_notes': 'https://docs.celonis.com/en/march-2023-release-notes.html',
            'february_2023_release_notes': 'https://docs.celonis.com/en/february-2023-release-notes.html',
            'january_2023_release_notes': 'https://docs.celonis.com/en/january-2023-release-notes.html',
            'release_notes_2022': 'https://docs.celonis.com/en/release-notes-2022.html',
            'december_2022_release_notes': 'https://docs.celonis.com/en/december-2022-release-notes.html',
            'november_2022_release_notes': 'https://docs.celonis.com/en/november-2022-release-notes.html',
            'october_2022_release_notes': 'https://docs.celonis.com/en/october-2022-release-notes.html',
            'september_2022_release_notes': 'https://docs.celonis.com/en/september-2022-release-notes.html',
            'august_2022_release_notes': 'https://docs.celonis.com/en/august-2022-release-notes.html',
            'july_2022_release_notes': 'https://docs.celonis.com/en/july-2022-release-notes.html',
            'june_2022_release_notes': 'https://docs.celonis.com/en/june-2022-release-notes.html',
            'may_2022_release_notes': 'https://docs.celonis.com/en/may-2022-release-notes.html',
            'april_2022_release_notes': 'https://docs.celonis.com/en/april-2022-release-notes.html'
        }
    
    def scrape_documentation(self, url: str, max_depth: int = 2) -> List[DocumentChunk]:
        """Scrape documentation from a given URL"""
        chunks = []
        visited = set()
        
        def scrape_page(current_url: str, depth: int = 0) -> List[DocumentChunk]:
            if depth > max_depth or current_url in visited:
                return []
            
            visited.add(current_url)
            page_chunks = []
            
            try:
                response = self.session.get(current_url, timeout=10)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract title
                title = soup.find('title')
                title_text = title.get_text().strip() if title else "Untitled"
                
                # Remove script and style elements
                for script in soup(["script", "style"]):
                    script.decompose()
                
                # Extract main content
                content_selectors = [
                    'main', 'article', '.content', '.documentation', 
                    '.markdown-body', '.wiki-content'
                ]
                
                main_content = None
                for selector in content_selectors:
                    main_content = soup.select_one(selector)
                    if main_content:
                        break
                
                if not main_content:
                    main_content = soup.find('body')
                
                if main_content:
                    # Extract text content in chunks
                    sections = self._extract_sections(main_content, title_text, current_url)
                    page_chunks.extend(sections)
                
                # Find related links for deeper scraping
                if depth < max_depth:
                    links = soup.find_all('a', href=True)
                    for link in links[:10]:  # Limit to prevent infinite scraping
                        href = link['href']
                        if self._is_relevant_link(href, current_url):
                            full_url = urljoin(current_url, href)
                            page_chunks.extend(scrape_page(full_url, depth + 1))
                
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error scraping {current_url}: {str(e)}")
            
            return page_chunks
        
        return scrape_page(url)
    
    def _extract_sections(self, content, title: str, url: str) -> List[DocumentChunk]:
        """Extract sections from HTML content"""
        chunks = []
        
        # Find all headings and their content
        headings = content.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        
        for i, heading in enumerate(headings):
            section_title = heading.get_text().strip()
            
            # Get content until next heading
            content_parts = []
            current = heading.next_sibling
            
            while current:
                if hasattr(current, 'name') and current.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                    break
                
                if hasattr(current, 'get_text'):
                    text = current.get_text().strip()
                    if text:
                        content_parts.append(text)
                
                current = current.next_sibling
            
            if content_parts:
                section_content = '\n'.join(content_parts)
                
                # Create chunks for large sections
                chunk_size = 1000
                if len(section_content) > chunk_size:
                    for j, chunk in enumerate(self._split_text(section_content, chunk_size)):
                        chunk_id = hashlib.md5(f"{url}_{section_title}_{j}".encode()).hexdigest()
                        chunks.append(DocumentChunk(
                            content=chunk,
                            url=url,
                            title=title,
                            section=f"{section_title} (Part {j+1})",
                            chunk_id=chunk_id,
                            timestamp=datetime.now()
                        ))
                else:
                    chunk_id = hashlib.md5(f"{url}_{section_title}".encode()).hexdigest()
                    chunks.append(DocumentChunk(
                        content=section_content,
                        url=url,
                        title=title,
                        section=section_title,
                        chunk_id=chunk_id,
                        timestamp=datetime.now()
                    ))
        
        return chunks
    
    def _split_text(self, text: str, chunk_size: int) -> List[str]:
        """Split text into chunks of specified size"""
        words = text.split()
        chunks = []
        current_chunk = []
        current_length = 0
        
        for word in words:
            if current_length + len(word) + 1 > chunk_size and current_chunk:
                chunks.append(' '.join(current_chunk))
                current_chunk = [word]
                current_length = len(word)
            else:
                current_chunk.append(word)
                current_length += len(word) + 1
        
        if current_chunk:
            chunks.append(' '.join(current_chunk))
        
        return chunks
    
    def _is_relevant_link(self, href: str, base_url: str) -> bool:
        """Determine if a link is relevant for PQL documentation"""
        if not href or href.startswith('#'):
            return False
        
        # Check for PQL-related keywords
        pql_keywords = ['pql', 'process', 'query', 'language', 'function', 'operator']
        href_lower = href.lower()
        
        for keyword in pql_keywords:
            if keyword in href_lower:
                return True
        
        # Check if it's a Celonis documentation link
        if 'docs.celonis.com' in href or 'community.celonis.com' in href:
            return True
        
        return False

class VectorStore:
    """Vector store for document embeddings using FAISS"""
    
    def __init__(self, model_name: str = 'all-MiniLM-L6-v2'):
        self.model = SentenceTransformer(model_name)
        self.index = None
        self.documents = []
        self.embeddings = None
    
    def add_documents(self, chunks: List[DocumentChunk]):
        """Add document chunks to the vector store"""
        if not chunks:
            return
        
        # Extract text content
        texts = [chunk.content for chunk in chunks]
        
        # Generate embeddings
        new_embeddings = self.model.encode(texts)
        
        if self.embeddings is None:
            self.embeddings = new_embeddings
            self.documents = chunks
        else:
            self.embeddings = np.vstack([self.embeddings, new_embeddings])
            self.documents.extend(chunks)
        
        # Build FAISS index
        self.index = faiss.IndexFlatIP(self.embeddings.shape[1])
        self.index.add(self.embeddings.astype(np.float32))
    
    def search(self, query: str, k: int = 5) -> List[Tuple[DocumentChunk, float]]:
        """Search for similar documents"""
        if self.index is None:
            return []
        
        query_embedding = self.model.encode([query])
        scores, indices = self.index.search(query_embedding.astype(np.float32), k)
        
        results = []
        for score, idx in zip(scores[0], indices[0]):
            if idx < len(self.documents):
                results.append((self.documents[idx], float(score)))
        
        return results
    
    def save(self, filepath: str):
        """Save vector store to file"""
        data = {
            'embeddings': self.embeddings,
            'documents': self.documents
        }
        with open(filepath, 'wb') as f:
            pickle.dump(data, f)
    
    def load(self, filepath: str):
        """Load vector store from file"""
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
        
        self.embeddings = data['embeddings']
        self.documents = data['documents']
        
        if self.embeddings is not None:
            self.index = faiss.IndexFlatIP(self.embeddings.shape[1])
            self.index.add(self.embeddings.astype(np.float32))

class PQLAgent:
    """AI Agent for answering PQL questions"""
    
    def __init__(self, vector_store: VectorStore, openai_api_key: str = None):
        self.vector_store = vector_store
        self.openai_api_key = openai_api_key
        if openai_api_key:
            openai.api_key = openai_api_key
    
    def answer_question(self, question: str, max_context_length: int = 3000) -> Dict:
        """Answer a PQL question using RAG"""
        # Search for relevant documents
        relevant_docs = self.vector_store.search(question, k=5)
        
        if not relevant_docs:
            return {
                'answer': "I couldn't find relevant information in the Celonis documentation. Please try rephrasing your question.",
                'sources': [],
                'confidence': 0.0
            }
        
        # Prepare context
        context_parts = []
        sources = []
        
        for doc, score in relevant_docs:
            if len('\n'.join(context_parts)) < max_context_length:
                context_parts.append(f"Source: {doc.title} - {doc.section}\n{doc.content}")
                sources.append({
                    'title': doc.title,
                    'section': doc.section,
                    'url': doc.url,
                    'score': score
                })
        
        context = '\n\n---\n\n'.join(context_parts)
        
        # Generate answer
        if self.openai_api_key:
            answer = self._generate_answer_with_openai(question, context)
        else:
            answer = self._generate_answer_simple(question, context, relevant_docs)
        
        return {
            'answer': answer,
            'sources': sources,
            'confidence': max([score for _, score in relevant_docs]) if relevant_docs else 0.0
        }
    
    def _generate_answer_with_openai(self, question: str, context: str) -> str:
        """Generate answer using OpenAI API"""
        try:
            # Updated to use the new OpenAI client
            from openai import OpenAI
            client = OpenAI(api_key=self.openai_api_key)
            
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {
                        "role": "system",
                        "content": """You are a Celonis PQL (Process Query Language) expert. 
                        Answer questions about PQL based on the provided documentation context. 
                        Be precise, provide code examples when relevant, and explain concepts clearly.
                        If the context doesn't contain enough information, say so."""
                    },
                    {
                        "role": "user",
                        "content": f"Context from Celonis documentation:\n{context}\n\nQuestion: {question}"
                    }
                ],
                max_tokens=500,
                temperature=0.3
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"OpenAI API error: {str(e)}")
            return self._generate_answer_simple(question, context, [])
    
    def _generate_answer_simple(self, question: str, context: str, relevant_docs: List) -> str:
        """Generate a simple answer without OpenAI API"""
        # Extract key information from the most relevant document
        if relevant_docs:
            best_doc, score = relevant_docs[0]
            
            # Look for code examples or function definitions
            content = best_doc.content
            
            # Simple pattern matching for PQL functions
            function_pattern = r'([A-Z_]+)\s*\([^)]*\)'
            functions = re.findall(function_pattern, content)
            
            answer_parts = [f"Based on the Celonis documentation from '{best_doc.title}':"]
            
            if functions:
                answer_parts.append(f"Relevant PQL functions: {', '.join(functions[:3])}")
            
            # Extract first few sentences as summary
            sentences = content.split('.')[:3]
            summary = '. '.join(sentences) + '.'
            answer_parts.append(summary)
            
            return '\n\n'.join(answer_parts)
        
        return "I found some relevant information, but couldn't generate a specific answer. Please check the sources below."

def main():
    st.set_page_config(
        page_title="Celonis PQL AI Agent",
        page_icon="🔍",
        layout="wide"
    )
    
    st.title("🔍 Celonis PQL AI Agent")
    st.markdown("*Ask questions about Celonis Process Query Language (PQL)*")
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("Configuration")
        
        # OpenAI API Key
        openai_key = st.text_input("OpenAI API Key (optional)", type="password")
        
        # Data source refresh
        if st.button("Refresh Documentation"):
            with st.spinner("Scraping Celonis documentation..."):
                refresh_documentation()
    
    # Initialize components
    vector_store = initialize_vector_store()
    agent = PQLAgent(vector_store, openai_key if openai_key else None)
    
    # Main interface
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("Ask a PQL Question")
        
        # Sample questions
        sample_questions = [
            "How do I calculate case duration in PQL?",
            "What is the difference between CASE_WHEN and CASE statement?",
            "How to use VARIANT function in PQL?",
            "What are the available aggregation functions in PQL?",
            "How to filter activities in PQL queries?"
        ]
        
        selected_sample = st.selectbox("Sample questions:", [""] + sample_questions)
        
        # User input
        user_question = st.text_area(
            "Your PQL question:",
            value=selected_sample if selected_sample else "",
            height=100,
            placeholder="e.g., How do I calculate the average case duration using PQL?"
        )
        
        if st.button("Get Answer", type="primary"):
            if user_question.strip():
                with st.spinner("Searching documentation and generating answer..."):
                    result = agent.answer_question(user_question)
                
                # Display answer
                st.subheader("Answer")
                st.write(result['answer'])
                
                # Display confidence
                confidence = result['confidence']
                st.metric("Confidence", f"{confidence:.2%}")
                
                # Display sources
                if result['sources']:
                    st.subheader("Sources")
                    for i, source in enumerate(result['sources'], 1):
                        with st.expander(f"Source {i}: {source['title']} - {source['section']}"):
                            st.write(f"**URL:** {source['url']}")
                            st.write(f"**Relevance Score:** {source['score']:.3f}")
            else:
                st.warning("Please enter a question.")
    
    with col2:
        st.header("Documentation Status")
        
        # Display vector store statistics
        if vector_store.documents:
            st.metric("Documents in Knowledge Base", len(vector_store.documents))
            
            # Show recent documents
            st.subheader("Recent Sources")
            recent_docs = sorted(vector_store.documents, key=lambda x: x.timestamp, reverse=True)[:5]
            
            for doc in recent_docs:
                st.write(f"📄 **{doc.title}**")
                st.write(f"   {doc.section}")
                st.write(f"   *Added: {doc.timestamp.strftime('%Y-%m-%d %H:%M')}*")
                st.write("---")
        else:
            st.info("No documents loaded. Use 'Refresh Documentation' to load data.")
        
        # PQL Quick Reference
        st.subheader("PQL Quick Reference")
        with st.expander("Common PQL Functions"):
            st.code("""
-- Case duration
DATEDIFF(dd, "Table"."Start", "Table"."End")

-- Activity occurrence
CASE WHEN "Table"."Activity" = 'Create Order' 
     THEN 1 ELSE 0 END

-- Variant analysis
VARIANT("Table"."Activity")

-- Filtering
FILTER "Table"."Status" = 'Completed'

-- Aggregations
COUNT("Table"."CaseID")
AVG("Table"."Duration")
SUM("Table"."Amount")
            """)

@st.cache_data
def initialize_vector_store():
    """Initialize or load the vector store"""
    vector_store = VectorStore()
    
    # Try to load existing data
    if os.path.exists("pql_knowledge_base.pkl"):
        try:
            vector_store.load("pql_knowledge_base.pkl")
            logger.info("Loaded existing knowledge base")
        except Exception as e:
            logger.error(f"Error loading knowledge base: {str(e)}")
            # Create new one if loading fails
            create_initial_knowledge_base(vector_store)
    else:
        create_initial_knowledge_base(vector_store)
    
    return vector_store

def create_initial_knowledge_base(vector_store: VectorStore):
    """Create initial knowledge base with sample PQL content"""
    # Sample PQL documentation chunks
    sample_chunks = [
        DocumentChunk(
            content="PQL (Process Query Language) is a domain-specific query language developed by Celonis for process mining. It allows you to translate your process-related questions into executable queries.",
            url="https://docs.celonis.com/en/pql---process-query-language.html",
            title="PQL - Process Query Language",
            section="Introduction",
            chunk_id="intro_1",
            timestamp=datetime.now()
        ),
        DocumentChunk(
            content="CASE_WHEN is used for conditional logic in PQL. Syntax: CASE WHEN condition THEN result ELSE alternative END. Example: CASE WHEN \"Activities\".\"Activity\" = 'Create Order' THEN 1 ELSE 0 END",
            url="https://docs.celonis.com/en/pql-function-library.html",
            title="PQL Function Library",
            section="Conditional Functions",
            chunk_id="case_when_1",
            timestamp=datetime.now()
        ),
        DocumentChunk(
            content="DATEDIFF calculates the difference between two dates. Syntax: DATEDIFF(unit, start_date, end_date). Units include: dd (days), hh (hours), mm (minutes), ss (seconds).",
            url="https://docs.celonis.com/en/pql-function-library.html",
            title="PQL Function Library",
            section="Date Functions",
            chunk_id="datediff_1",
            timestamp=datetime.now()
        ),
        DocumentChunk(
            content="VARIANT function returns the variant of a case, which is the sequence of activities. Syntax: VARIANT(\"Table\".\"Activity_Column\"). This is useful for process variant analysis.",
            url="https://docs.celonis.com/en/pql-function-library.html",
            title="PQL Function Library",
            section="Process Functions",
            chunk_id="variant_1",
            timestamp=datetime.now()
        ),
        DocumentChunk(
            content="COUNT function counts the number of rows. COUNT(column) counts non-null values, COUNT(*) counts all rows. COUNT(DISTINCT column) counts unique values.",
            url="https://docs.celonis.com/en/pql-function-library.html",
            title="PQL Function Library",
            section="Aggregation Functions",
            chunk_id="count_1",
            timestamp=datetime.now()
        )
    ]
    
    vector_store.add_documents(sample_chunks)
    
    # Save the initial knowledge base
    try:
        vector_store.save("pql_knowledge_base.pkl")
        logger.info("Created and saved initial knowledge base")
    except Exception as e:
        logger.error(f"Error saving knowledge base: {str(e)}")

def refresh_documentation():
    """Refresh documentation from Celonis sources"""
    scraper = CelonisDocScraper()
    vector_store = VectorStore()
    
    all_chunks = []
    
    # Scrape main documentation URLs
    for name, url in scraper.doc_urls.items():
        st.write(f"Scraping {name}...")
        try:
            chunks = scraper.scrape_documentation(url, max_depth=1)
            all_chunks.extend(chunks)
            st.write(f"Found {len(chunks)} chunks from {name}")
        except Exception as e:
            st.error(f"Error scraping {name}: {str(e)}")
    
    if all_chunks:
        vector_store.add_documents(all_chunks)
        vector_store.save("pql_knowledge_base.pkl")
        st.success(f"Successfully updated knowledge base with {len(all_chunks)} document chunks!")
        st.rerun()  # Updated from st.experimental_rerun()
    else:
        st.warning("No new content was scraped.")

if __name__ == "__main__":
    main()
