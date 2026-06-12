import json
from operator import truediv
from typing import List, Dict, Any
from pydantic import BaseModel
from dataclasses import dataclass


# class DynamicFormInsertBatchCmd(BaseModel):
#     collectionName: str
#     data: List[Dict[str, Any]]
#     shouldExecuteFS: bool
#     isGlobalRequest: bool = False
#     canSplitPerSite: bool = True
#     isBAPublishable: bool = False
#     formPrefix: str = "FRM_"
#
#     @property
#     def get_schema_name(self) -> str:
#         """Equivalent to getSchemaName in Scala"""
#         return self.collectionName.replace(self.formPrefix, "")
#
#     @property
#     def get_collection_name(self) -> str:
#         """Equivalent to getCollectionName in Scala"""
#         if self.collectionName.startswith(self.formPrefix):
#             return self.collectionName
#         return self.formPrefix + self.collectionName
#
#     def to_json(self) -> str:
#         """Equivalent to toJson in Scala"""
#         # For Pydantic v2 use model_dump_json(), for Pydantic v1 use json()
#         if hasattr(self, 'model_dump_json'):
#             return self.model_dump_json()
#         return self.json()

class DynamicFormInsertBatchCmd(BaseModel):
    collectionName: str
    data: List[Dict[str, Any]]
    shouldExecuteFs: bool
    isGlobalRequest: bool = False
    canSplitPerSite: bool = True
    isBAPublishable: bool = False
    formPrefix: str = "FRM_"


@property
def get_schema_name(self) -> str:
    return self.collectionName.replace(self.formPrefix, "")


@property
def getCollectionName(self) -> str:
    if self.collectionName.startswith(self.formPrefix):
        return self.collectionName
    else:
        return self.formPrefix + self.collectionName


@property
def toJson(self) -> str: self.json() #todo need to revisit again
