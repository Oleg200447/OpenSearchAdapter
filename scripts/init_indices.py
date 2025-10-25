"""Script to initialize system indices in OpenSearch."""
import asyncio
import sys
from pathlib import Path

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import settings
from src.opensearch_client import opensearch_client
from src.logger import logger


async def init_system_indices():
    """Initialize all system topic indices."""
    logger.info("Starting system indices initialization...")
    
    # Connect to OpenSearch
    connected = await asyncio.to_thread(opensearch_client.connect)
    if not connected:
        logger.error("Failed to connect to OpenSearch")
        return False
    
    # Create system indices
    success = await asyncio.to_thread(opensearch_client.create_system_indices)
    
    if success:
        logger.info("All system indices initialized successfully")
    else:
        logger.error("Some indices failed to initialize")
    
    # Close connection
    await asyncio.to_thread(opensearch_client.close)
    
    return success


def main():
    """Main entry point."""
    print("OpenSearch Adapter - System Indices Initialization")
    print("=" * 60)
    print(f"\nOpenSearch: {settings.opensearch_url}")
    print(f"System Topics: {', '.join(settings.system_topics_list)}")
    print(f"Embedding Dimension: {settings.embedding_dimension}")
    print("\nInitializing indices...\n")
    
    # Run async initialization
    success = asyncio.run(init_system_indices())
    
    if success:
        print("\n✓ System indices initialized successfully!")
        print("\nCreated indices:")
        for topic in settings.system_topics_list:
            print(f"  - system_{topic}")
        return 0
    else:
        print("\n✗ Failed to initialize some indices")
        print("Check logs for details")
        return 1


if __name__ == "__main__":
    sys.exit(main())
