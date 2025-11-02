import re
from typing import List
from src.config import settings
from src.logger import logger


class TextChunker:
    def __init__(
        self,
        chunk_size_words: int = None,
        overlap_words: int = None
    ):
        self.chunk_size_words = chunk_size_words or settings.chunk_size_words
        self.overlap_words = overlap_words or settings.chunk_overlap_words
        
        # Sentence boundary regex for Russian and English
        # Matches periods, exclamation marks, question marks followed by space/newline
        self.sentence_pattern = re.compile(r'[.!?]+[\s\n]+')
        
    def split_into_words(self, text: str) -> List[str]:
        # Split on whitespace but keep the text structure
        words = re.findall(r'\S+|\s+', text)
        # Filter out pure whitespace entries but keep words with punctuation
        return [w for w in words if w.strip()]
    
    def chunk_text(self, text: str) -> List[str]:
        if not text or not text.strip():
            logger.warning("Empty text provided for chunking")
            return []
        
        # Split into sentences first for better semantic boundaries
        sentences = self._split_into_sentences(text)
        
        if not sentences:
            logger.warning("No sentences found in text")
            return [text]
        
        chunks = []
        current_chunk_words = []
        current_word_count = 0
        
        for sentence in sentences:
            sentence_words = self.split_into_words(sentence)
            sentence_word_count = len(sentence_words)
            
            # If single sentence exceeds chunk size, split it by words
            if sentence_word_count > self.chunk_size_words:
                # Save current chunk if it exists
                if current_chunk_words:
                    chunks.append(' '.join(current_chunk_words).strip())
                    current_chunk_words = []
                    current_word_count = 0

                # Split large sentence into chunks
                for i in range(0, sentence_word_count, self.chunk_size_words - self.overlap_words):
                    chunk_words = sentence_words[i:i + self.chunk_size_words]
                    chunks.append(' '.join(chunk_words).strip())
                continue
            
            # Check if adding this sentence would exceed chunk size
            if current_word_count + sentence_word_count > self.chunk_size_words:
                # Save current chunk
                if current_chunk_words:
                    chunks.append(' '.join(current_chunk_words).strip())
                
                # Start new chunk with overlap from previous chunk
                if self.overlap_words > 0 and current_chunk_words:
                    # Take last N words from previous chunk
                    overlap_start = max(0, len(current_chunk_words) - self.overlap_words)
                    current_chunk_words = current_chunk_words[overlap_start:]
                    current_word_count = len([w for w in current_chunk_words if w.strip()])
                else:
                    current_chunk_words = []
                    current_word_count = 0
            
            # Add sentence to current chunk
            current_chunk_words.extend(sentence_words)
            current_word_count += sentence_word_count
        
        # Add final chunk
        if current_chunk_words:
            chunks.append(' '.join(current_chunk_words).strip())
        
        logger.info(f"Text chunked into {len(chunks)} chunks")
        return chunks
    
    def _split_into_sentences(self, text: str) -> List[str]:
        sentences = []
        current = ""
        for part in re.split(r'([.!?]+[\s\n]+)', text):
            current += part
            if re.search(r'[.!?]+\s*$', current):
                sentences.append(current.strip())
                current = ""
        if current and current.strip():
            sentences.append(current.strip())
        return sentences

chunker = TextChunker()
