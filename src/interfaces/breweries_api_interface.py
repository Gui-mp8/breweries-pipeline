from abc import ABC, abstractmethod

class BreweriesAPIInterface(ABC):
    def __init__(self):
        self._base_url = None

    @property
    def base_url(self) -> str:
        return self._base_url

    @base_url.setter
    def base_url(self, endpoint: str) -> None:
        self._base_url = endpoint