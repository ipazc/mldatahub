import unittest
import os
import shutil
from mldatahub.storage.local.local_storage import LocalStorage


class TestLocalStorage(unittest.TestCase):

    def setUp(self):
        self.temp_path = "examples/tmp_folder"

    def test_storage_creates_folder(self):
        """
        Tests whether the storage is able to create a folder in the specified directory.
        """
        if os.path.exists(self.temp_path):
            shutil.rmtree(self.temp_path)

        storage = LocalStorage(self.temp_path)

        self.assertTrue(os.path.exists(self.temp_path))

    def test_storage_creates_read_file(self):
        """
        Tests whether the storage is able to create a file and read it after.
        """

        storage = LocalStorage(self.temp_path)
        file_id = storage.put_file_content(b"content")

        self.assertTrue(os.path.exists(os.path.join(self.temp_path, str(file_id))))
        self.assertEqual(file_id, 0)

        content = storage.get_file_content(file_id)

        self.assertEqual(content, b"content")

    def test_storage_get_invalid_id(self):
        """
        Tests that the storage raises exception on invalid id.
        """
        storage = LocalStorage(self.temp_path)

        with self.assertRaises(Exception) as ex:
            storage.get_file_content("aaaa")
            self.assertEqual(
                "File ID is not valid for local storage, must be an integer.",
                str(ex.exception)
            )

        with self.assertRaises(Exception) as ex:
            storage = LocalStorage(self.temp_path, "aaa")
            self.assertEqual(
                "File ID is not valid for local storage, must be an integer.",
                str(ex.exception)
            )

        with self.assertRaises(Exception) as ex:
            storage.put_file_content(b"content", "aaa")
            self.assertEqual(
                "File ID is not valid for local storage, must be an integer.",
                str(ex.exception)
            )

    def tearDown(self):
        shutil.rmtree(self.temp_path)

if __name__ == '__main__':
    unittest.main()
