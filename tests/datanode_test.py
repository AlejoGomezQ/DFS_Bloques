import os
import sys
import unittest
import tempfile
import shutil

# Add the src directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.datanode.storage.block_storage import BlockStorage

class TestBlockStorage(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.storage = BlockStorage(self.temp_dir)
        
    def tearDown(self):
        shutil.rmtree(self.temp_dir)
    
    def test_store_and_retrieve_block(self):
        block_id = "test_block_1"
        block_data = b"This is test block data"
        
        # Store the block
        result = self.storage.store_block(block_id, block_data)
        self.assertTrue(result)
        
        # Check if block exists
        self.assertTrue(self.storage.block_exists(block_id))
        
        # Retrieve the block
        retrieved_data = self.storage.retrieve_block(block_id)
        self.assertEqual(retrieved_data, block_data)
        
        # Get block size
        size = self.storage.get_block_size(block_id)
        self.assertEqual(size, len(block_data))
        
        # Calculate checksum
        checksum = self.storage.calculate_checksum(block_id)
        self.assertIsNotNone(checksum)
    
    def test_delete_block(self):
        block_id = "test_block_2"
        block_data = b"This is another test block"
        
        # Store the block
        self.storage.store_block(block_id, block_data)
        self.assertTrue(self.storage.block_exists(block_id))
        
        # Delete the block
        result = self.storage.delete_block(block_id)
        self.assertTrue(result)
        
        # Verify block no longer exists
        self.assertFalse(self.storage.block_exists(block_id))
    
    def test_get_all_blocks(self):
        # Store multiple blocks
        blocks = {
            "block1": b"Data for block 1",
            "block2": b"Data for block 2",
            "block3": b"Data for block 3"
        }
        
        for block_id, data in blocks.items():
            self.storage.store_block(block_id, data)
        
        # Get all blocks
        all_blocks = self.storage.get_all_blocks()
        self.assertEqual(len(all_blocks), 3)
        
        # Check that all block IDs are in the list
        for block_id in blocks.keys():
            self.assertIn(block_id, all_blocks)
    
    def test_storage_stats(self):
        # Store some blocks
        blocks = {
            "stats_block1": b"Data for stats block 1",
            "stats_block2": b"Data for stats block 2"
        }
        
        for block_id, data in blocks.items():
            self.storage.store_block(block_id, data)
        
        # Get storage stats
        stats = self.storage.get_storage_stats()
        
        self.assertEqual(stats["total_blocks"], 2)
        self.assertEqual(stats["total_size"], sum(len(data) for data in blocks.values()))
        self.assertEqual(len(stats["blocks"]), 2)
    
    def test_stream_block(self):
        block_id = "stream_block"
        block_data = b"This is data for streaming test" * 10  # Make it larger
        
        # Store the block
        self.storage.store_block(block_id, block_data)
        
        # Stream the block with small chunk size
        chunks = self.storage.stream_block(block_id, chunk_size=20)
        
        # Reconstruct the data
        reconstructed_data = b""
        total_size = None
        
        for chunk, offset, size in chunks:
            reconstructed_data += chunk
            if total_size is None:
                total_size = size
        
        self.assertEqual(reconstructed_data, block_data)
        self.assertEqual(total_size, len(block_data))

if __name__ == "__main__":
    unittest.main()
