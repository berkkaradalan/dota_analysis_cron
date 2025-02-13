import logging
from retrying import retry
from pymongo.mongo_client import MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from pydantic import BaseModel
from datetime import datetime
import requests
import os
from typing import Optional, List
from enum import Enum

load_dotenv()

class env_variables():
    MONGODB_URL = os.getenv("MONGODB_URL")

class AttackTypes(str, Enum):
    MELEE = "Melee"
    RANGED = "Ranged"

class HeroRoles(str, Enum):
    CARRY = "Carry"
    ESCAPE = "Escape"
    NUKER = "Nuker"
    INITIATOR = "Initiator"
    DURABLE = "Durable"
    SUPPORT = "Support"
    DISABLER = "Disabler"
    PUSHER = "Pusher"

class Hero(BaseModel):
    HeroID:         str
    HeroName:       str
    HeroRoles:      List[HeroRoles]
    AttackType:     AttackTypes
    HeroImageURL:   str

class TopHeroes(BaseModel):
    AccountID:      str
    HeroID:         str
    LastPlayed:     float
    Games:          int
    GamesWon:       int

class Match(BaseModel):
    AccountID:         str
    MatchID:           str
    PlayerSlot:        str
    RadiantWin:        bool
    Duration:          str
    GameMode:          str
    HeroID:            str
    StartTime:         datetime
    Kills:             int
    Deaths:            int
    Assists:           int

class User(BaseModel):
    AccountID:         str
    PersonaName:       str
    Name:              Optional[str]
    SteamID:           str
    LastLogin:         Optional[datetime]
    SteamAvatar:       str

class UserWinLoose(BaseModel):
    AccountID:         str
    Win:                int
    Lose:              int

class DetailedMatch(BaseModel):
    MatchID:           str
    AccountID:         str
    HeroID:            str
    Item0:             int
    Item1:             int
    Item2:             int
    Item3:             int
    Item4:             int
    Item5:             int
    Kills:             int
    Assists:           int
    Death:             int
    LastHits:          int
    Denies:            int
    GoldPerMinute:     int
    XPPerMinute:       int
    Level:             int
    NetWorth:          int
    HeroDamage:        int
    TowerDamage:       int
    HeroHealing:       int
    Gold:              int
    GoldSpent:         int
    AbilityUpgrades:   List[int]
    MatchStartTime:    float
    MatchDuration:     float
    GameMode:          int
    IsRadiant:         bool
    RadiantWin:        bool
    Win:               bool
    Lose:              bool
    KillDeathAssist:   float
    CollectionHash:    str

def retry_logging_function(exception):
    log_error(f"Retrying due to exception: {exception}")
    return True

def setup_logger(log_file="error.log"):
    logging.basicConfig(
        filename=log_file,
        level=logging.ERROR,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

def log_error(error_message):
    logging.error(error_message)

@retry(stop_max_attempt_number=100, retry_on_exception=retry_logging_function)
def connect_to_mongodb():
    client = MongoClient(env_variables.MONGODB_URL, server_api=ServerApi('1'), tlsAllowInvalidCertificates=True)
    return client

@retry(stop_max_attempt_number=10, retry_on_exception=retry_logging_function)
def send_request_to_open_dota(url:str):
    return requests.get(url).json()

@retry(stop_max_attempt_number=10, retry_on_exception=retry_logging_function)
def migarte_mongodb(mongodb:MongoClient):
    collections = mongodb.list_collection_names()
    if "user" not in collections:
        mongodb.create_collection("user")
        user_collection = mongodb["user"]
        user_collection.create_index("AccountID", unique=True)
    if "hero" not in collections:
        mongodb.create_collection("hero")
        hero_collection = mongodb["hero"]
        hero_collection.create_index("HeroID", unique=True)
    if "match" not in collections:
        mongodb.create_collection("match")
        match_collection = mongodb["match"]
        match_collection.create_index("MatchID", unique=True)
    if "winlose" not in collections:
        mongodb.create_collection("winlose")
        win_lose_collection = mongodb["winlose"]
        win_lose_collection.create_index("AccountID", unique=True)
    if "favorite_heroes" not in collections:
        mongodb.create_collection("favorite_heroes")
    if "detailed_match" not in collections:
        mongodb.create_collection("detailed_match")
        detailed_match_collection = mongodb["detailed_match"]
        detailed_match_collection.create_index("CollectionHash", unique=True)

@retry(stop_max_attempt_number=10, retry_on_exception=retry_logging_function)
def get_all_collections(mongodb:MongoClient):
    user_collection = mongodb["user"]
    hero_collection = mongodb["hero"]
    match_collection = mongodb["match"]
    win_lose_collection = mongodb["winlose"]
    favorite_heroes_collection = mongodb["favorite_heroes"]
    detailed_match_collection = mongodb["detailed_match"]
    return user_collection, hero_collection, match_collection, win_lose_collection, favorite_heroes_collection, detailed_match_collection

@retry(stop_max_attempt_number=10, retry_on_exception=retry_logging_function)
def get_all_user_ids(user_collection):
    users = user_collection.find()
    return [user["AccountID"] for user in users]

@retry(stop_max_attempt_number=10, retry_on_exception=retry_logging_function)
def update_matchs_of_users(user_ids: list, match_collection, limit=100):
    for user_id in user_ids:
        offset = 0
        while True:
            user_matches = requests.get(
                f"https://api.opendota.com/api/players/{user_id}/matches?limit={limit}&offset={offset}"
            ).json()
            if not user_matches:
                break
            for user_match in user_matches:
                try:
                    match_collection.insert_one(Match(
                        AccountID=user_id,
                        MatchID=str(user_match["match_id"]),
                        PlayerSlot=str(user_match["player_slot"]),
                        RadiantWin=user_match["radiant_win"],
                        Duration=str(user_match["duration"]),
                        GameMode=str(user_match["game_mode"]),
                        HeroID=str(user_match["hero_id"]),
                        StartTime=user_match["start_time"],
                        Kills=user_match["kills"],
                        Deaths=user_match["deaths"],
                        Assists=user_match["assists"],
                    ).model_dump())
                except DuplicateKeyError:
                    pass
                except Exception as e:
                    log_error(f"Unexpected error : {e}")
            offset += limit

@retry(stop_max_attempt_number=10, retry_on_exception=retry_logging_function)
@retry(stop_max_attempt_number=10)
def update_wins_loses_of_user(user_ids:list, win_lose_collection, limit=100):
    for user_id in user_ids:
        user_win_lose = requests.get(f"https://api.opendota.com/api/players/{user_id}/wl").json()
        if "win" not in user_win_lose:
            return
        try:
            win_lose_collection.insert_one(UserWinLoose(
                    AccountID=user_id,
                    Win=user_win_lose["win"],
                    Lose=user_win_lose["lose"]
            ).model_dump())
        except DuplicateKeyError:
            pass
        except Exception as e:
            log_error(f"Unexpected error : {e}")

@retry(stop_max_attempt_number=10, retry_on_exception=retry_logging_function)
def update_detailed_match_collection_of_user(user_ids: list, match_collection, detailed_match_collection, limit=100, offset=0):
    for user_id in user_ids:
        current_offset = offset
        while True:
            user_matches = match_collection.find().skip(current_offset).limit(limit)
            matches_list = list(user_matches)
            if not matches_list:
                break
            for user_match in matches_list:
                detailed_match = requests.get(f"https://api.opendota.com/api/matches/{user_match['MatchID']}").json()
                detailed_match = detailed_match.get("players", [])
                detailed_match = next((player for player in detailed_match if player.get('account_id') == int(user_id)), None)
                if detailed_match:
                    try:
                        detailed_match_collection.insert_one(DetailedMatch(
                            MatchID=user_match["MatchID"],
                            AccountID=user_id,
                            HeroID=str(detailed_match["hero_id"]),
                            Item0=detailed_match["item_0"],
                            Item1=detailed_match["item_1"],
                            Item2=detailed_match["item_2"],
                            Item3=detailed_match["item_3"],
                            Item4=detailed_match["item_4"],
                            Item5=detailed_match["item_5"],
                            Kills=detailed_match["kills"],
                            Assists=detailed_match["assists"],
                            Death=detailed_match["deaths"],
                            LastHits=detailed_match["last_hits"],
                            Denies=detailed_match["denies"],
                            GoldPerMinute=detailed_match["gold_per_min"],
                            XPPerMinute=detailed_match["xp_per_min"],
                            Level=detailed_match["level"],
                            NetWorth=detailed_match["net_worth"],
                            HeroDamage=detailed_match["hero_damage"],
                            TowerDamage=detailed_match["tower_damage"],
                            HeroHealing=detailed_match["hero_healing"],
                            Gold=detailed_match["gold"],
                            GoldSpent=detailed_match["gold_spent"],
                            AbilityUpgrades=detailed_match["ability_upgrades_arr"],
                            MatchStartTime=detailed_match["start_time"],
                            MatchDuration=detailed_match["duration"],
                            GameMode=detailed_match["game_mode"],
                            IsRadiant=detailed_match["isRadiant"],
                            RadiantWin=detailed_match["radiant_win"],
                            Win=detailed_match["win"],
                            Lose=detailed_match["lose"],
                            KillDeathAssist=detailed_match["kda"],
                            CollectionHash=str(user_match["MatchID"] + user_id)
                        ).model_dump())
                    except DuplicateKeyError:
                        pass
                    except Exception as e:
                        log_error(f"Unexpected error : {e}")
            current_offset += limit

@retry(stop_max_attempt_number=10, retry_on_exception=retry_logging_function)
def update_hero_collection(hero_collection):
    heroes = send_request_to_open_dota(url="https://api.opendota.com/api/heroes")
    if "localized_name" not in heroes:
        return
    for hero in heroes:
        try:
            hero_collection.insert_one(Hero(
                HeroID = str(hero["id"]),
                HeroName = hero["localized_name"],
                HeroRoles = hero["roles"],
                AttackType = hero["attack_type"],
                HeroImageURL = f"https://cdn.dota2.com/apps/dota2/images/heroes/{hero['name'].replace('npc_dota_hero_', '')}_full.png"
            ).model_dump())
        except DuplicateKeyError:
            pass
        except Exception as e:
            log_error(f"Unexpected error : {e}")

@retry(stop_max_attempt_number=10, retry_on_exception=retry_logging_function)
def update_favorite_heroes_of_users(user_ids:list, favorite_heroes_collection):
    for user_id in user_ids:
        favorite_heroes = send_request_to_open_dota(url=f"https://api.opendota.com/api/players/{user_id}/heroes")
        if len(favorite_heroes)<0:
            return
        
        try:
            top_heroes = sorted(favorite_heroes, key=lambda x: x["games"], reverse=True)[:3]
            for hero in top_heroes:
                favorite_heroes_collection.insert_one(
                    TopHeroes(TopHeroes(
                    AccountID=user_id,
                    HeroID=str(hero["hero_id"]),
                    LastPlayed=hero["last_played"],
                    Games=hero["games"],
                    GamesWon=hero["win"])).model_dump())
        except DuplicateKeyError:
            pass
        except Exception as e:
            log_error(f"Unexpected error : {e}")

@retry(stop_max_attempt_number=10, retry_on_exception=retry_logging_function)
def main_func():
    mongodb_connection = connect_to_mongodb()
    mongodb = mongodb_connection["dota_analysis"]
    migarte_mongodb(mongodb=mongodb)
    setup_logger()
    user_collection, hero_collection, match_collection, win_lose_collection, favorite_heroes_collection, detailed_match_collection = get_all_collections(mongodb=mongodb)
    user_ids = get_all_user_ids(user_collection=user_collection)
    update_matchs_of_users(user_ids=user_ids, match_collection=match_collection)
    update_wins_loses_of_user(user_ids=user_ids, win_lose_collection=win_lose_collection)
    update_detailed_match_collection_of_user(user_ids=user_ids, match_collection=match_collection, detailed_match_collection=detailed_match_collection)
    update_hero_collection(user_ids=user_ids, hero_collection=hero_collection)
    update_favorite_heroes_of_users(user_ids=user_ids, favorite_heroes_collection=favorite_heroes_collection)


main_func()