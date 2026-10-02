from dataclasses import dataclass, field, fields
from typing import Self


@dataclass
class PlayTask:
    type: str = '' # Observed values: "FileTask", "URLTask"
    category: str = 'game' # Observed values: "game", "document". Probably <https://docs.gog.com/bc-file-tasks/#:~:text=below%3A-,Category>
    languages: list[str] = field(default_factory=list)
    name: str = ''
    isPrimary: bool = False

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        # Filter out unknown keys (some FileTasks have an empty "url" parameter, for example)
        return cls(**{
            key: value
            for key, value in data.items()
            if key in list(map(lambda x: x.name, fields(cls)))
        })


@dataclass
class FileTask(PlayTask):
    arguments: str | None = None
    path: str = ''
    workingDir: str = ''


@dataclass
class URLTask(PlayTask):
    link: str = ''


@dataclass
class InfoEntry:
    buildId: str
    gameId: str
    language: str
    languages: list[str]
    name: str
    playTasks: list[FileTask | URLTask]
    rootGameId: str
    version: int
    clientId: str | None = None

    @classmethod
    def from_dict(cls, data: dict) -> Self:
        def deserialize_play_task(pt_data: dict) -> FileTask | URLTask:
            match pt_data.get('type', None):
                case 'FileTask':
                    return FileTask.from_dict(pt_data)
                case 'URLTask':
                    return URLTask.from_dict(pt_data)
                case _:
                    raise ValueError(f'Unknown play task type encountered in {pt_data}')

        modified = data.copy()
        modified['playTasks'] = list(map(deserialize_play_task, data['playTasks']))
        return cls(**modified)
