#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# MLDataHub
# Copyright (C) 2017 Iván de Paz Centeno <ipazc@unileon.es>.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 3
# of the License or (at your option) any later version of
# the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA  02110-1301, USA.

from mldatahub.config.config import global_config
global_config.set_session_uri("mongodb://localhost:27017/unittests")
global_config.set_page_size(2)
from mldatahub.factory.dataset_factory import DatasetFactory
from mldatahub.factory.dataset_element_factory import DatasetElementFactory
from werkzeug.exceptions import Unauthorized, BadRequest, RequestedRangeNotSatisfiable, NotFound, Conflict
from mldatahub.config.privileges import Privileges
import unittest
from mldatahub.odm.dataset_dao import DatasetDAO, DatasetCommentDAO, DatasetElementDAO, DatasetElementCommentDAO
from mldatahub.odm.file_dao import FileDAO
from mldatahub.odm.token_dao import TokenDAO


__author__ = 'Iván de Paz Centeno'

storage = global_config.get_storage()


class TestDatasetElementFactory(unittest.TestCase):

    def setUp(self):
        self.session = global_config.get_session()
        DatasetDAO.query.remove()
        DatasetCommentDAO.query.remove()
        DatasetElementDAO.query.remove()
        DatasetElementCommentDAO.query.remove()
        FileDAO.query.remove()
        TokenDAO.query.remove()

    def test_dataset_element_creation(self):
        """
        Factory can create dataset's elements.
        """
        anonymous = TokenDAO("Anonymous", 1, 1, "anonymous")

        creator = TokenDAO("normal user privileged with link", 1, 1, "user1",
                           privileges=Privileges.CREATE_DATASET + Privileges.ADD_ELEMENTS
                           )
        creator2 = TokenDAO("normal user unprivileged", 1, 1, "user1",
                           privileges=Privileges.CREATE_DATASET
                           )
        admin = TokenDAO("admin user", 1, 1, "admin", privileges=Privileges.ADMIN_CREATE_TOKEN + Privileges.ADMIN_EDIT_TOKEN + Privileges.ADMIN_DESTROY_TOKEN)

        dataset = DatasetDAO("user1/dataset1", "example_dataset", "dataset for testing purposes", "none", tags=["example", "0"])
        dataset2 = DatasetDAO("user1/dataset2", "example_dataset2", "dataset2 for testing purposes", "none", tags=["example", "1"])

        self.session.flush()

        creator = creator.link_dataset(dataset)
        creator2 = creator2.link_dataset(dataset)

        # Creator can create elements into the dataset
        element = DatasetElementFactory(creator, dataset).create_element(title="New element", description="Description unknown",
                                                               tags=["example_tag"], content=b"hello")

        self.assertEqual(element.tags, ["example_tag"])

        content = storage.get_file(element.file_ref_id).content
        self.assertEqual(content, b"hello")
        self.session.flush()
        # Creator can't create elements referencing existing files directly (Exploit fix)
        with self.assertRaises(Unauthorized) as ex:
            element = DatasetElementFactory(creator, dataset).create_element(title="New element2",
                                                                             description="Description unknown2",
                                                                             tags=["example_tag"],
                                                                             file_ref_id=element.file_ref_id)

        # Creator can't create elements on other's datasets if not linked with them
        with self.assertRaises(Unauthorized) as ex:
            element = DatasetElementFactory(creator, dataset2).create_element(title="New element2",
                                                                              description="Description unknown2",
                                                                              tags=["example_tag"], content=b"hello2")

        # Anonymous can't create elements
        with self.assertRaises(Unauthorized) as ex:
            element = DatasetElementFactory(anonymous, dataset).create_element(title="New element3", description="Description unknown",
                                                               tags=["example_tag"], content=b"hello3")

        # Creator2, even linked to the dataset, can't create elements as it is not privileged
        with self.assertRaises(Unauthorized) as ex:
            element = DatasetElementFactory(creator2, dataset).create_element(title="New element4",
                                                                              description="Description unknown4",
                                                                              tags=["example_tag"], content=b"hello4")

        # Admin can do any of the previous actions.
        element = DatasetElementFactory(admin, dataset).create_element(title="New element5",
                                                                          description="Description unknown5",
                                                                          tags=["example_tag"], content=b"hello5")
        self.session.flush_all()
        self.session.refresh(dataset)

        dataset = DatasetDAO.query.get(_id=dataset._id)
        self.assertIn(dataset._id, element.dataset_id)
        self.assertEqual(len(dataset.elements), 2)

        new_element = DatasetElementFactory(admin, dataset2).create_element(title="New element6",
                                                                  description="Description unknown5",
                                                                  tags=["example_tag"], file_ref_id=element.file_ref_id)

        self.assertEqual(element.file_ref_id, new_element.file_ref_id)


    def test_dataset_elements_creation(self):
        """
        Factory can create multiple dataset's elements at once.
        """
        creator = TokenDAO("normal user privileged with link", 1, 3, "user1",
                           privileges=Privileges.CREATE_DATASET + Privileges.ADD_ELEMENTS
                           )

        dataset = DatasetDAO("user1/dataset1", "example_dataset", "dataset for testing purposes", "none", tags=["example", "0"])

        self.session.flush()

        creator = creator.link_dataset(dataset)

        # Creator can create elements into the dataset
        elements_kwargs = [
            dict(title="New element", description="Description unknown", tags=["example_tag"], content=b"hello"),
            dict(title="New element2", description="Description unknown2", tags=["example_tag2"], content=b"hello2"),
            dict(title="New element3", description="Description unknown3", tags=["example_tag3"], content=b"hello3"),
        ]

        elements = DatasetElementFactory(creator, dataset).create_elements(elements_kwargs)

        self.assertEqual(len(elements), 3)

        content = storage.get_file(elements[0].file_ref_id).content
        self.assertEqual(content, b"hello")
        content = storage.get_file(elements[1].file_ref_id).content
        self.assertEqual(content, b"hello2")
        content = storage.get_file(elements[2].file_ref_id).content
        self.assertEqual(content, b"hello3")
        dataset = dataset.update()

        self.assertEqual(len(dataset.elements), 3)

    def test_dataset_element_removal(self):
        """
        Factory can remove elements from datasets.
        """
        anonymous = TokenDAO("Anonymous", 1, 1, "anonymous")

        destructor = TokenDAO("normal user privileged with link", 1, 1, "user1",
                           privileges=Privileges.DESTROY_DATASET + Privileges.DESTROY_ELEMENTS
                           )
        destructor2 = TokenDAO("normal user unprivileged", 1, 1, "user1",
                           privileges=Privileges.DESTROY_DATASET
                           )
        admin = TokenDAO("admin user", 1, 1, "admin", privileges=Privileges.ADMIN_CREATE_TOKEN + Privileges.ADMIN_EDIT_TOKEN + Privileges.ADMIN_DESTROY_TOKEN)

        dataset = DatasetDAO("user1/dataset1", "example_dataset", "dataset for testing purposes", "none", tags=["example", "0"])
        dataset2 = DatasetDAO("user1/dataset2", "example_dataset2", "dataset2 for testing purposes", "none", tags=["example", "1"])

        self.session.flush()

        destructor = destructor.link_dataset(dataset)
        destructor2 = destructor2.link_dataset(dataset2)

        file_id1 = storage.put_file_content(b"content1")
        file_id2 = storage.put_file_content(b"content2")

        element  = DatasetElementDAO("example1", "none", file_id1, dataset=dataset)
        element2 = DatasetElementDAO("example2", "none", file_id1, dataset=dataset)
        element3 = DatasetElementDAO("example3", "none", file_id2, dataset=dataset2)

        self.session.flush()
        dataset = dataset.update()
        dataset2 = dataset2.update()

        self.assertEqual(len(dataset.elements), 2)
        self.assertEqual(len(dataset2.elements), 1)

        # Destructor can not destroy elements from a dataset that is not linked to
        with self.assertRaises(Unauthorized) as ex:
            DatasetElementFactory(destructor, dataset2).destroy_element(element._id)

        with self.assertRaises(Unauthorized) as ex:
            DatasetElementFactory(destructor, dataset2).destroy_element(element2._id)

        with self.assertRaises(Unauthorized) as ex:
            DatasetElementFactory(destructor, dataset2).destroy_element(element3._id)

        # Destructor can not destroy elements if they exist but are not inside his dataset
        with self.assertRaises(Unauthorized) as ex:
            DatasetElementFactory(destructor, dataset).destroy_element(element3._id)

        # Destructor can not destroy elements if they don't exist
        with self.assertRaises(Unauthorized) as ex:
            DatasetElementFactory(destructor, dataset).destroy_element("randomID")

        # Destructor can destroy elements if they exist and are  inside his dataset
        DatasetElementFactory(destructor, dataset).destroy_element(element._id)

        # Even though element is destroyed, file referenced should still exist
        self.assertEqual(storage.get_file(file_id1).content, b"content1")

        dataset = dataset.update()

        self.assertEqual(len(dataset.elements), 1)

        # Admin can remove elements form any source
        DatasetElementFactory(admin, dataset).destroy_element(element2._id)
        DatasetElementFactory(admin, dataset2).destroy_element(element3._id)

        self.session.flush()

        dataset = dataset.update()
        dataset2 = dataset2.update()

        self.assertEqual(len(dataset.elements), 0)
        self.assertEqual(len(dataset2.elements), 0)

    def test_dataset_elements_removal(self):
        """
        Factory can remove mutliple elements from datasets at once.
        """
        destructor = TokenDAO("normal user privileged with link", 1, 1, "user1",
                           privileges=Privileges.DESTROY_DATASET + Privileges.DESTROY_ELEMENTS
                           )


        dataset = DatasetDAO("user1/dataset1", "example_dataset", "dataset for testing purposes", "none", tags=["example", "0"])

        self.session.flush()

        destructor = destructor.link_dataset(dataset)

        file_id1 = storage.put_file_content(b"content1")
        file_id2 = storage.put_file_content(b"content2")

        element  = DatasetElementDAO("example1", "none", file_id1, dataset=dataset)
        element2 = DatasetElementDAO("example2", "none", file_id1, dataset=dataset)
        element3 = DatasetElementDAO("example3", "none", file_id2, dataset=dataset)

        self.session.flush()
        dataset = dataset.update()

        self.assertEqual(len(dataset.elements), 3)

        with self.assertRaises(RequestedRangeNotSatisfiable) as ex:
            DatasetElementFactory(destructor, dataset).destroy_elements([element._id, element2._id, element3._id])

        DatasetElementFactory(destructor, dataset).destroy_elements([element._id, element2._id])

        dataset = dataset.update()

        self.assertEqual(len(dataset.elements), 1)
        self.assertEqual(dataset.elements[0]._id, element3._id)

        element  = DatasetElementDAO("example1", "none", file_id1, dataset=dataset)
        element2 = DatasetElementDAO("example2", "none", file_id1, dataset=dataset)

        self.session.flush()
        dataset = dataset.update()

        DatasetElementFactory(destructor, dataset).destroy_elements()

    def test_dataset_element_edit(self):
        """
        Factory can edit elements from datasets.
        """
        editor = TokenDAO("normal user privileged with link", 1, 1, "user1",
                           privileges=Privileges.EDIT_DATASET + Privileges.EDIT_ELEMENTS
                           )
        editor2 = TokenDAO("normal user unprivileged", 1, 1, "user1",
                           privileges=Privileges.EDIT_DATASET
                           )
        admin = TokenDAO("admin user", 1, 1, "admin", privileges=Privileges.ADMIN_CREATE_TOKEN + Privileges.ADMIN_EDIT_TOKEN + Privileges.ADMIN_DESTROY_TOKEN)

        dataset = DatasetDAO("user1/dataset1", "example_dataset", "dataset for testing purposes", "none", tags=["example", "0"])
        dataset2 = DatasetDAO("user1/dataset2", "example_dataset2", "dataset2 for testing purposes", "none", tags=["example", "1"])

        self.session.flush()

        editor = editor.link_dataset(dataset)
        editor2 = editor2.link_dataset(dataset2)

        file_id1 = storage.put_file_content(b"content1")
        file_id2 = storage.put_file_content(b"content2")

        element  = DatasetElementDAO("example1", "none", file_id1, dataset=dataset)
        element2 = DatasetElementDAO("example2", "none", file_id1, dataset=dataset)
        element3 = DatasetElementDAO("example3", "none", file_id2, dataset=dataset2)

        self.session.flush()
        dataset = dataset.update()
        dataset2 = dataset2.update()

        self.assertEqual(len(dataset.elements), 2)
        self.assertEqual(len(dataset2.elements), 1)

        # editor can not edit elements from a dataset that is not linked to
        with self.assertRaises(Unauthorized) as ex:
            DatasetElementFactory(editor, dataset2).edit_element(element._id, title="asd")

        with self.assertRaises(Unauthorized) as ex:
            DatasetElementFactory(editor, dataset2).edit_element(element2._id, title="asd2")

        with self.assertRaises(Unauthorized) as ex:
            DatasetElementFactory(editor, dataset2).edit_element(element3._id, title="asd3")

        # editor can not edit elements if they exist but are not inside his dataset
        with self.assertRaises(Unauthorized) as ex:
            DatasetElementFactory(editor, dataset).edit_element(element3._id, title="asd4")

        # editor can not edit elements if they don't exist
        with self.assertRaises(NotFound) as ex:
            DatasetElementFactory(editor, dataset).edit_element("randomID", title="asd5")

        # Editor can edit elements if they exist and are inside his dataset
        DatasetElementFactory(editor, dataset).edit_element(element._id, title="asd6")

        self.session.flush()

        dataset = dataset.update()
        element = element.update()

        # Editor can not change references to files
        with self.assertRaises(Unauthorized) as ex:
            DatasetElementFactory(editor, dataset).edit_element(element._id, file_ref_id="other_reference")

        # BUT he can change the content
        DatasetElementFactory(editor, dataset).edit_element(element._id, content=b"other_content")

        element = element.update()
        self.assertEqual(storage.get_file(element.file_ref_id).content, b"other_content")

        # Admin can do whatever he wants
        DatasetElementFactory(admin, dataset).edit_element(element2._id, title="changed by admin")
        element2 = element2.update()
        self.assertEqual(element2.title, "changed by admin")

        DatasetElementFactory(admin, dataset2).edit_element(element3._id, file_ref_id=element.file_ref_id)

        element3 = element3.update()
        self.assertEqual(storage.get_file(element3.file_ref_id).content,
                         storage.get_file(element.file_ref_id).content)

        self.session.flush()

    def test_dataset_element_view(self):
        """
        Factory can view elements from datasets.
        """
        editor = TokenDAO("normal user privileged with link", 1, 1, "user1",
                          privileges=Privileges.RO_WATCH_DATASET
                          )
        editor2 = TokenDAO("normal user unprivileged", 1, 1, "user1",
                           privileges=0
                           )
        admin = TokenDAO("admin user", 1, 1, "admin",
                         privileges=Privileges.ADMIN_CREATE_TOKEN + Privileges.ADMIN_EDIT_TOKEN + Privileges.ADMIN_DESTROY_TOKEN)

        dataset = DatasetDAO("user1/dataset1", "example_dataset", "dataset for testing purposes", "none",
                             tags=["example", "0"])
        dataset2 = DatasetDAO("user1/dataset2", "example_dataset2", "dataset2 for testing purposes", "none",
                              tags=["example", "1"])

        self.session.flush()

        editor = editor.link_dataset(dataset)
        editor2 = editor2.link_dataset(dataset2)

        file_id1 = storage.put_file_content(b"content1")
        file_id2 = storage.put_file_content(b"content2")

        element = DatasetElementDAO("example1", "none", file_id1, dataset=dataset)
        element2 = DatasetElementDAO("example2", "none", file_id1, dataset=dataset)
        element3 = DatasetElementDAO("example3", "none", file_id2, dataset=dataset2)

        self.session.flush()
        dataset = dataset.update()
        dataset2 = dataset2.update()

        self.assertEqual(len(dataset.elements), 2)
        self.assertEqual(len(dataset2.elements), 1)

        # editor can see elements from his dataset
        element_watched = DatasetElementFactory(editor, dataset).get_element_info(element._id)
        self.assertEqual(element_watched.title, "example1")

        # editor can not see elements from other's datasets
        with self.assertRaises(Unauthorized) as ex:
            element_watched = DatasetElementFactory(editor, dataset2).get_element_info(element3._id)

        # editor can not see external elements within his dataset
        with self.assertRaises(Unauthorized) as ex:
            element_watched = DatasetElementFactory(editor, dataset).get_element_info(element3._id)

        # editor2 is not privileged and can not see any elements of his own dataset
        with self.assertRaises(Unauthorized) as ex:
            element_watched = DatasetElementFactory(editor2, dataset2).get_element_info(element3._id)

        # Or external elements
        with self.assertRaises(Unauthorized) as ex:
            element_watched = DatasetElementFactory(editor2, dataset2).get_element_info(element2._id)

        # Or other datasets
        with self.assertRaises(Unauthorized) as ex:
            element_watched = DatasetElementFactory(editor2, dataset).get_element_info(element2._id)

        # Admin can do anything
        element_watched = DatasetElementFactory(admin, dataset).get_element_info(element._id)
        self.assertEqual(element_watched.title, "example1")

        # But not this: dataset2 does not have element
        with self.assertRaises(Unauthorized) as ex:
            element_watched = DatasetElementFactory(admin, dataset2).get_element_info(element._id)

        element_watched = DatasetElementFactory(admin, dataset2).get_element_info(element3._id)
        self.assertEqual(element_watched.title, "example3")

    def test_dataset_element_creation_limit(self):
        """
        Factory limits creation of dataset's elements depending on the token used to create it.
        """
        creator = TokenDAO("normal user privileged", 1, 1, "user1", privileges=Privileges.CREATE_DATASET+Privileges.ADD_ELEMENTS)

        dataset = DatasetDAO("user1/dataset1", "example_dataset", "dataset for testing purposes", "none",
                             tags=["example", "0"])

        self.session.flush()

        # Creator should not be able to create more than 1 element in any dataset
        creator = creator.link_dataset(dataset)

        element1 = DatasetElementFactory(creator, dataset).create_element(title="New element1", description="Description unknown",
                                                               tags=["example_tag"], content=b"hello")
        dataset = dataset.update()

        with self.assertRaises(Unauthorized) as ex:
            element2 = DatasetElementFactory(creator, dataset).create_element(title="New element2",
                                                                              description="Description unknown",
                                                                              tags=["example_tag"], content=b"hello")

    def test_dataset_elements_info_by_pages(self):
        """
        Factory can retrieve multiple elements at once by pages.
        """

        editor = TokenDAO("normal user privileged with link", 1, 5, "user1",
                          privileges=Privileges.RO_WATCH_DATASET
                          )

        dataset = DatasetDAO("user1/dataset1", "example_dataset", "dataset for testing purposes", "none",
                             tags=["example", "0"])

        self.session.flush()

        editor = editor.link_dataset(dataset)

        elements = [DatasetElementDAO("example{}".format(x), "none", None, dataset=dataset).title for x in range(5)]

        self.session.flush()

        dataset = dataset.update()

        page_size = global_config.get_page_size()
        for page in range(len(elements) // page_size + int(len(elements) % page_size > 0)):
            retrieved_elements = DatasetElementFactory(editor, dataset).get_elements_info(page)

            for x in retrieved_elements:
                self.assertIn(x.title, elements)

    def test_dataset_elements_info_by_different_pages_size(self):
        """
        Factory can retrieve multiple elements with different page sizes.
        """
        initial_page_size = global_config.get_page_size()
        global_config.set_page_size(10)

        editor = TokenDAO("normal user privileged with link", 1, 5, "user1",
                          privileges=Privileges.RO_WATCH_DATASET
                          )

        dataset = DatasetDAO("user1/dataset1", "example_dataset", "dataset for testing purposes", "none",
                             tags=["example", "0"])

        self.session.flush()

        editor = editor.link_dataset(dataset)

        elements = [DatasetElementDAO("example{}".format(x), "none", None, dataset=dataset).title for x in range(5)]

        self.session.flush()

        dataset = dataset.update()

        # We need to know the order of the elements
        ordered_elements = [l for l in DatasetElementFactory(editor, dataset).get_elements_info(page_size=len(elements))]

        pages_size = [1, 2, 3, 4, 5]

        for page_size in pages_size:
            num_pages = len(elements) // page_size + int(len(elements) % page_size > 0)

            for page in range(num_pages):
                retrieved_elements = [l for l in DatasetElementFactory(editor, dataset).get_elements_info(page, page_size=page_size)]
                for retrieved_element, ordered_element in zip(retrieved_elements, ordered_elements[page*page_size:(page+1)*page_size]):
                    self.assertEqual(retrieved_element._id, ordered_element._id)

        with self.assertRaises(Conflict):
            retrieved_elements = DatasetElementFactory(editor, dataset).get_elements_info(page_size=global_config.get_page_size()+1)

        global_config.set_page_size(initial_page_size)

    def test_dataset_specific_elements_info(self):
        """
        Factory can retrieve multiple elements at once by specific sets.
        """

        editor = TokenDAO("normal user privileged with link", 1, 1, "user1",
                          privileges=Privileges.RO_WATCH_DATASET
                          )

        dataset = DatasetDAO("user1/dataset1", "example_dataset", "dataset for testing purposes", "none",
                             tags=["example", "0"])

        self.session.flush()

        editor = editor.link_dataset(dataset)

        elements = [DatasetElementDAO("example{}".format(x), "none", None, dataset=dataset) for x in range(5)]
        titles = [element.title for element in elements]
        ids = [element._id for element in elements]

        self.session.flush()

        dataset = dataset.update()

        # Can't retrieve more elements than the page size at once.
        with self.assertRaises(RequestedRangeNotSatisfiable) as ex:
            retrieved_elements = [x for x in DatasetElementFactory(editor, dataset).get_specific_elements_info(ids)]

        request1 = ids[0:2]
        retrieved_elements = [x for x in DatasetElementFactory(editor, dataset).get_specific_elements_info(request1)]

        self.assertEqual(len(retrieved_elements), 2)
        self.assertIn(retrieved_elements[0].title, titles)
        self.assertIn(retrieved_elements[1].title, titles)

        request2 = ids[1:3]
        retrieved_elements2 = [x for x in DatasetElementFactory(editor, dataset).get_specific_elements_info(request2)]
        self.assertEqual(len(retrieved_elements2), 2)
        self.assertIn(retrieved_elements2[0].title, titles)
        self.assertIn(retrieved_elements2[1].title, titles)
        self.assertNotEqual(retrieved_elements[0].title, retrieved_elements2[0].title)
        self.assertNotEqual(retrieved_elements[1].title, retrieved_elements2[1].title)
        self.assertEqual(retrieved_elements[1].title, retrieved_elements2[0].title)

    def test_dataset_element_content_retrieval(self):
        """
        Factory can retrieve content of an element.
        """
        editor = TokenDAO("normal user privileged with link", 1, 1, "user1",
                          privileges=Privileges.RO_WATCH_DATASET
                          )
        editor2 = TokenDAO("normal user unprivileged", 1, 1, "user1",
                           privileges=0
                           )
        admin = TokenDAO("admin user", 1, 1, "admin",
                         privileges=Privileges.ADMIN_CREATE_TOKEN + Privileges.ADMIN_EDIT_TOKEN + Privileges.ADMIN_DESTROY_TOKEN)

        dataset = DatasetDAO("user1/dataset1", "example_dataset", "dataset for testing purposes", "none",
                             tags=["example", "0"])
        dataset2 = DatasetDAO("user1/dataset2", "example_dataset2", "dataset2 for testing purposes", "none",
                              tags=["example", "1"])

        self.session.flush()

        editor = editor.link_dataset(dataset)
        editor2 = editor2.link_dataset(dataset2)

        file_id1 = storage.put_file_content(b"content1")
        file_id2 = storage.put_file_content(b"content2")

        element = DatasetElementDAO("example1", "none", file_id1, dataset=dataset)
        element2 = DatasetElementDAO("example2", "none", file_id1, dataset=dataset)
        element3 = DatasetElementDAO("example3", "none", file_id2, dataset=dataset2)

        self.session.flush()
        dataset = dataset.update()
        dataset2 = dataset2.update()

        self.assertEqual(len(dataset.elements), 2)
        self.assertEqual(len(dataset2.elements), 1)

        # editor can see elements from his dataset
        element_content = DatasetElementFactory(editor, dataset).get_element_content(element._id)
        self.assertEqual(element_content, b"content1")

        # editor can not see elements from other's datasets
        with self.assertRaises(Unauthorized) as ex:
            element_content = DatasetElementFactory(editor, dataset2).get_element_content(element3._id)

        # editor can not see external elements within his dataset
        with self.assertRaises(Unauthorized) as ex:
            element_content = DatasetElementFactory(editor, dataset).get_element_content(element3._id)

        # editor2 is not privileged and can not see any elements of his own dataset
        with self.assertRaises(Unauthorized) as ex:
            element_content = DatasetElementFactory(editor2, dataset2).get_element_content(element3._id)

        # Or external elements
        with self.assertRaises(Unauthorized) as ex:
            element_content = DatasetElementFactory(editor2, dataset2).get_element_content(element2._id)

        # Or other datasets
        with self.assertRaises(Unauthorized) as ex:
            element_content = DatasetElementFactory(editor2, dataset).get_element_content(element2._id)

        # Admin can do anything
        element_content = DatasetElementFactory(admin, dataset).get_element_content(element._id)
        self.assertEqual(element_content, b"content1")

        # But not this: dataset2 does not have element
        with self.assertRaises(Unauthorized) as ex:
            element_content = DatasetElementFactory(admin, dataset2).get_element_content(element._id)

        element_content = DatasetElementFactory(admin, dataset2).get_element_content(element3._id)
        self.assertEqual(element_content, b"content2")

    def test_dataset_multiple_elements_content_retrieval(self):
        """
        Factory can retrieve content of multiple elements at once.
        """
        editor = TokenDAO("normal user privileged with link", 1, 1, "user1",
                          privileges=Privileges.RO_WATCH_DATASET
                          )

        dataset = DatasetDAO("user1/dataset1", "example_dataset", "dataset for testing purposes", "none",
                             tags=["example", "0"])

        self.session.flush()

        editor = editor.link_dataset(dataset)

        file_id1 = storage.put_file_content(b"content1")
        file_id2 = storage.put_file_content(b"content2")

        element = DatasetElementDAO("example1", "none", file_id1, dataset=dataset)
        element2 = DatasetElementDAO("example2", "none", file_id1, dataset=dataset)
        element3 = DatasetElementDAO("example3", "none", file_id2, dataset=dataset)

        self.session.flush()
        dataset = dataset.update()

        with self.assertRaises(RequestedRangeNotSatisfiable) as ex:
            contents = DatasetElementFactory(editor, dataset).get_elements_content([element._id, element2._id, element3._id])

        contents = DatasetElementFactory(editor, dataset).get_elements_content([element._id, element3._id])

        self.assertEqual(contents[element._id], b"content1")
        self.assertEqual(contents[element3._id], b"content2")


    def test_dataset_elements_edit(self):
        """
        Factory can edit multiple elements from datasets at once.
        """
        editor = TokenDAO("normal user privileged with link", 1, 1, "user1",
                           privileges=Privileges.EDIT_DATASET + Privileges.EDIT_ELEMENTS
                           )

        dataset = DatasetDAO("user1/dataset1", "example_dataset", "dataset for testing purposes", "none", tags=["example", "0"])

        self.session.flush()

        editor = editor.link_dataset(dataset)

        file_id1 = storage.put_file_content(b"content1")
        file_id2 = storage.put_file_content(b"content2")

        element  = DatasetElementDAO("example1", "none", file_id1, dataset=dataset)
        element2 = DatasetElementDAO("example2", "none", file_id1, dataset=dataset)
        element3 = DatasetElementDAO("example3", "none", file_id2, dataset=dataset)

        self.session.flush()
        dataset = dataset.update()

        self.assertEqual(len(dataset.elements), 3)

        modifications = {
            element._id: dict(title="asd6", content=b"content4"),
            element3._id: dict(description="ffff", content=b"New Content!")
        }

        DatasetElementFactory(editor, dataset).edit_elements(modifications)

        self.session.flush()

        dataset = dataset.update()
        element = element.update()
        element3 = element3.update()

        self.assertEqual(element.title, "asd6")
        self.assertEqual(storage.get_file(element.file_ref_id).content, b"content4")
        self.assertEqual(element3.description, "ffff")
        self.assertEqual(storage.get_file(element3.file_ref_id).content, b"New Content!")

    def test_clone_element(self):
        """
        Factory can clone elements.
        """
        editor = TokenDAO("normal user privileged with link", 2, 1, "user1",
                         privileges=Privileges.RO_WATCH_DATASET + Privileges.EDIT_DATASET + Privileges.ADD_ELEMENTS
                         )

        dataset = DatasetDAO("user1/dataset1", "example_dataset", "dataset for testing purposes", "none",
                             tags=["example", "0"])
        dataset2 = DatasetDAO("user1/dataset2", "example_dataset2", "dataset2 for testing purposes", "none",
                              tags=["example", "1"])

        self.session.flush()

        editor = editor.link_dataset(dataset)

        file_id1 = storage.put_file_content(b"content1")
        file_id2 = storage.put_file_content(b"content2")

        element = DatasetElementDAO("example1", "none", file_id1, dataset=dataset)
        element2 = DatasetElementDAO("example2", "none", file_id1, dataset=dataset)
        element3 = DatasetElementDAO("example3", "none", file_id2, dataset=dataset)

        self.session.flush()
        dataset = dataset.update()
        dataset2 = dataset2.update()

        self.assertEqual(len(dataset.elements), 3)
        self.assertEqual(len(dataset2.elements), 0)

        with self.assertRaises(Unauthorized) as ex:
            new_element = DatasetElementFactory(editor, dataset).clone_element(element._id, dataset2.url_prefix)

        editor = editor.link_dataset(dataset2)
        self.assertEqual(len(element.dataset_id), 1)
        new_element = DatasetElementFactory(editor, dataset).clone_element(element._id, dataset2.url_prefix)

        dataset2 = dataset2.update()

        self.assertEqual(len(dataset2.elements), 1)

        self.assertEqual(new_element.file_ref_id, element.file_ref_id)
        self.assertEqual(new_element.title, element.title)
        self.assertEqual(new_element.description, element.description)
        self.assertEqual(new_element.tags, element.tags)
        self.assertEqual(new_element._id, element._id)
        self.assertEqual(len(new_element.dataset_id), 2)
        self.assertEqual(len(element.dataset_id), 2)

        with self.assertRaises(Unauthorized) as ex:
            new_element = DatasetElementFactory(editor, dataset).clone_element(element2._id, dataset2.url_prefix)

    def test_clone_elements(self):
        """
        Factory can clone multiple elements at once.
        """
        editor = TokenDAO("normal user privileged with link", 2, 3, "user1",
                         privileges=Privileges.RO_WATCH_DATASET + Privileges.EDIT_DATASET + Privileges.ADD_ELEMENTS
                         )

        dataset = DatasetDAO("user1/dataset1", "example_dataset", "dataset for testing purposes", "none",
                             tags=["example", "0"])
        dataset2 = DatasetDAO("user1/dataset2", "example_dataset2", "dataset2 for testing purposes", "none",
                              tags=["example", "1"])

        self.session.flush()

        editor = editor.link_dataset(dataset)

        file_id1 = storage.put_file_content(b"content1")
        file_id2 = storage.put_file_content(b"content2")

        element = DatasetElementDAO("example1", "none", file_id1, dataset=dataset)
        element2 = DatasetElementDAO("example2", "none", file_id1, dataset=dataset)
        element3 = DatasetElementDAO("example3", "none", file_id2, dataset=dataset)

        self.session.flush()
        dataset = dataset.update()
        dataset2 = dataset2.update()

        self.assertEqual(len(dataset.elements), 3)
        self.assertEqual(len(dataset2.elements), 0)

        with self.assertRaises(Unauthorized) as ex:
            new_elements = DatasetElementFactory(editor, dataset).clone_elements([element._id, element2._id], dataset2.url_prefix)

        editor = editor.link_dataset(dataset2)
        new_elements = DatasetElementFactory(editor, dataset).clone_elements([element._id, element2._id],
                                                                            dataset2.url_prefix)

        dataset2 = dataset2.update()

        self.assertEqual(len(dataset.elements), 3)
        self.assertEqual(len(dataset2.elements), 2)

    def test_dataset_fork_element_modification(self):
        """
        Elements modified from a forked dataset do not affect the main dataset.
        :return:
        """
        editor = TokenDAO("normal user privileged with link", 100, 200, "user1",
                     privileges=Privileges.RO_WATCH_DATASET + Privileges.CREATE_DATASET + Privileges.EDIT_DATASET +
                                Privileges.ADD_ELEMENTS + Privileges.EDIT_ELEMENTS + Privileges.DESTROY_ELEMENTS
                 )

        main_dataset = DatasetFactory(editor).create_dataset(url_prefix="foobar", title="foo", description="bar",
                                                             reference="none", tags=["a"])

        editor = editor.link_dataset(main_dataset)

        elements_proto = [{
            'title': 't{}'.format(i),
            'description': 'desc{}'.format(i),
            'http_ref': 'none',
            'tags': ['none'],
            'content': "content{}".format(i).encode()
        } for i in range(4)]

        elements = [DatasetElementFactory(editor, main_dataset).create_element(**element_proto) for element_proto in elements_proto]
        self.session.flush()

        self.assertEqual(len(elements), len(main_dataset.elements))
        forked_dataset = DatasetFactory(editor).fork_dataset(main_dataset.url_prefix, editor, url_prefix="foo")
        editor.link_dataset(forked_dataset)
        self.session.flush()
        self.assertEqual(len(forked_dataset.elements), len(main_dataset.elements))
        DatasetElementFactory(editor, forked_dataset).destroy_element(forked_dataset.elements[0]._id)

        self.session.flush()
        self.assertEqual(len(forked_dataset.elements), len(main_dataset.elements)-1)
        self.assertEqual(forked_dataset.elements[0].title, "t1")

        DatasetElementFactory(editor, main_dataset).destroy_element(main_dataset.elements[1]._id)

        self.session.flush()
        self.assertEqual(len(forked_dataset.elements), len(main_dataset.elements))
        self.assertEqual(forked_dataset.elements[0].title, "t1")
        self.assertEqual(forked_dataset.elements[1].title, "t2")
        self.assertEqual(forked_dataset.elements[2].title, "t3")
        self.assertEqual(main_dataset.elements[0].title, "t0")
        self.assertEqual(main_dataset.elements[1].title, "t2")
        self.assertEqual(main_dataset.elements[2].title, "t3")

        DatasetElementFactory(editor, forked_dataset).edit_element(main_dataset.elements[2]._id, title='tc2')
        self.session.flush()

        self.assertEqual(main_dataset.elements[2].title, "t3")
        self.assertEqual(forked_dataset.elements[2].title, "tc2")

        DatasetElementFactory(editor, main_dataset).edit_element(main_dataset.elements[1]._id, title='tc')
        self.session.flush()

        self.assertEqual(main_dataset.elements[1].title, "tc")
        self.assertEqual(forked_dataset.elements[1].title, "t2")

        main_dataset = DatasetFactory(editor).create_dataset(url_prefix="foobar2", title="foo", description="bar",
                                                             reference="none", tags=["a"])

        editor.link_dataset(main_dataset)
        self.session.flush()

        elements_proto = [{
            'title': 't{}'.format(i),
            'description': 'desc{}'.format(i),
            'http_ref': 'none',
            'tags': ['none'],
            'content': "content{}".format(i).encode()
        } for i in range(4)]

        elements = [DatasetElementFactory(editor, main_dataset).create_element(**element_proto) for element_proto in elements_proto]
        self.session.flush()

        forked_dataset2 = DatasetFactory(editor).fork_dataset(main_dataset.url_prefix, editor, url_prefix="bar")
        self.session.flush()
        editor.link_dataset(forked_dataset2)
        self.session.flush()
        mod_proto = {
            main_dataset.elements[0]._id: dict(title="t3"),
            main_dataset.elements[1]._id: dict(title="t3_2"),
        }

        self.assertEqual(forked_dataset2.elements[0]._id, main_dataset.elements[0]._id)
        self.assertEqual(forked_dataset2.elements[1]._id, main_dataset.elements[1]._id)
        self.assertEqual(forked_dataset2.elements[2]._id, main_dataset.elements[2]._id)
        self.assertEqual(forked_dataset2.elements[3]._id, main_dataset.elements[3]._id)

        DatasetElementFactory(editor, main_dataset).edit_elements(mod_proto)
        self.session.flush()
        self.assertEqual(forked_dataset2.elements[0].title, "t0")
        self.assertEqual(forked_dataset2.elements[1].title, "t1")
        self.assertEqual(forked_dataset2.elements[2].title, "t2")
        self.assertEqual(forked_dataset2.elements[3].title, "t3")
        self.assertEqual(main_dataset.elements[0].title, "t3")
        self.assertEqual(main_dataset.elements[1].title, "t3_2")
        self.assertEqual(main_dataset.elements[2].title, "t2")
        self.assertEqual(main_dataset.elements[3].title, "t3")

        self.assertNotEqual(forked_dataset2.elements[0]._id, main_dataset.elements[0]._id)
        self.assertNotEqual(forked_dataset2.elements[1]._id, main_dataset.elements[1]._id)
        self.assertEqual(forked_dataset2.elements[2]._id, main_dataset.elements[2]._id)
        self.assertEqual(forked_dataset2.elements[3]._id, main_dataset.elements[3]._id)

        mod_proto = {
            forked_dataset2.elements[2]._id: dict(title="t4"),
            forked_dataset2.elements[3]._id: dict(title="t4_2"),
        }
        DatasetElementFactory(editor, forked_dataset2).edit_elements(mod_proto)

        self.assertNotEqual(forked_dataset2.elements[0]._id, main_dataset.elements[0]._id)
        self.assertNotEqual(forked_dataset2.elements[1]._id, main_dataset.elements[1]._id)
        self.assertNotEqual(forked_dataset2.elements[2]._id, main_dataset.elements[2]._id)
        self.assertNotEqual(forked_dataset2.elements[3]._id, main_dataset.elements[3]._id)
        self.assertEqual(forked_dataset2.elements[2].title, "t4")
        self.assertEqual(forked_dataset2.elements[3].title, "t4_2")
        self.assertEqual(main_dataset.elements[2].title, "t2")
        self.assertEqual(main_dataset.elements[3].title, "t3")

    def test_dataset_discover_new_ids_for_changed_ids_of_elements(self):
        """
        The IDs of the elements modified from a forked dataset can be discovered from old ID.
        :return:
        """
        editor = TokenDAO("normal user privileged with link", 100, 200, "user1",
                     privileges=Privileges.RO_WATCH_DATASET + Privileges.CREATE_DATASET + Privileges.EDIT_DATASET +
                                Privileges.ADD_ELEMENTS + Privileges.EDIT_ELEMENTS + Privileges.DESTROY_ELEMENTS
                 )

        main_dataset = DatasetFactory(editor).create_dataset(url_prefix="foobar", title="foo", description="bar",
                                                             reference="none", tags=["a"])

        editor = editor.link_dataset(main_dataset)

        elements_proto = [{
            'title': 't{}'.format(i),
            'description': 'desc{}'.format(i),
            'http_ref': 'none',
            'tags': ['none'],
            'content': "content{}".format(i).encode()
        } for i in range(4)]

        elements = [DatasetElementFactory(editor, main_dataset).create_element(**element_proto) for element_proto in elements_proto]
        self.session.flush()

        self.assertEqual(len(elements), len(main_dataset.elements))
        forked_dataset = DatasetFactory(editor).fork_dataset(main_dataset.url_prefix, editor, url_prefix="foo")
        editor.link_dataset(forked_dataset)
        self.session.flush()
        self.assertEqual(len(forked_dataset.elements), len(main_dataset.elements))

        previous_id = forked_dataset.elements[0]._id

        DatasetElementFactory(editor, forked_dataset).edit_element(forked_dataset.elements[0]._id, title="edited")
        print(forked_dataset.elements[0]._id)

        result = DatasetElementFactory(editor, forked_dataset).discover_real_id([previous_id])
        print(result)

        print(forked_dataset.elements[0].title)

    def tearDown(self):
        DatasetDAO.query.remove()
        DatasetCommentDAO.query.remove()
        DatasetElementDAO.query.remove()
        DatasetElementCommentDAO.query.remove()
        TokenDAO.query.remove()
        FileDAO.query.remove()

    @classmethod
    def tearDownClass(cls):
        storage.delete()

if __name__ == '__main__':
    unittest.main()
