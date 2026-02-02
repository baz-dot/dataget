"""
Drama ID -> Drama Name 映射表
从 QuickBI 历史数据同步
"""

# drama_id -> drama_name 映射
DRAMA_MAPPING = {
    "15000001": "Happily Ever After Together",
    "15000002": "Class In Hypnosis",
    "15000018": "Love in the Apocalypse",
    "15000022": "My Man's Man",
    "15000031": "Honey or Money",
    "15000033": "Ban the Nineteens",
    "15000040": "Rule #1: No Dating",
    "15000053": "Real-Life Dating 101",
    "15000062": "My Best Friend's little brother",
    "15000074": "I Love My Best Friend",
    "15000079": "Tendering Resignation",
    "15000085": "Crush on Zombie",
    "15000091": "Match Play",
    "15000092": "Gang Girl Goes to Boy's School",
    "15000104": "The Bedmate Game Sharehouse",
    "15000105": "Actors High",
    "15000108": "You Want Some?",
    "15000109": "Dear X",
    "15000140": "Time-Leap Romance: Revenge Swap",
    "15000162": "Be Still My Heart",
    "15000176": "Am I the Villain?!",
    "15000201": "I Slept With My Sister's Fiancé",
    "15000208": "Love is after the contract",
    "15000223": "The Freaky Exchange",
    "15000227": "Fight for Love",
    "15000232": "Blood Mate",
    "15000250": "The Night You Erased",
    "15000264": "Fake Marriage with My CEO Bestie",
    "15000276": "Con Crush: My duty to you is eternal",
    "15000279": "Love Strike",
    "15000287": "Sold to Love",
    "15000289": "Older Than His Alibi",
    "15000296": "Eternal Love after One-Night Stand?",
    "15000298": "Swipe Wright: Dating 101",
    "15000302": "I Accidentally Slept With My Professor",
    "15000305": "Platinum - Behind their lies",
    "15000315": "Under the hood",
    "15000316": "Matrimoney",
    "15000319": "A Vampire In The Alpha's Den",
    "15000323": "Mated to My Rival Alpha",
    "15000324": "Save Me with Latte Kiss",
    "15000327": "Private Affair: Falling for A Secret Billionaire",
    "15000330": "My Best Friend's Game: Vengeance on the Devil",
    "15000342": "Fate: The Servant of the Night",
    "15000343": "Good Girl Gone Bad",
    "15000344": "Law Firm Romance: The Attorney and the Secretary",
    "15000350": "The Bedmate Game Sharehouse 2",
    "15000351": "The Secret Life of Amy Bensen",
    "15000398": "Mile High on Cloud Nine",
    "15000400": "Camp Evergreen",
    "15000431": "Met a Savior in Hell",
    "15000467": "The Summer I Fell for My Butler",
    "15000468": "Romantic Island",
    "15000469": "The Blind Bride of The Scarred Mafia Boss",
    "15000472": "The Bedmate Game Sharehouse 2",
    "15000473": "The Bedmate Game Sharehouse 2",
    "15000497": "I Slept With My Dad's Best Friend",
    "15000500": "Marked By My Casino Heir",
    "15000529": "Quarterback's Team Doctor",
    "15000628": "How to get out of novel",
    "15000694": "One Night, One Destiny",
    "15000696": "Was It Just a Coincidence... or Fate?",
    "15000727": "Ideal Boyfriend: 5 Handsome Guys and a Girl's Kiss",
    "15000728": "Eldest Daughter's Marriage Life",
    "15000729": "You'll Regret What You Did to the Lost Heiress!",
    "15000730": "Find Me, Mr. President!",
    "15000760": "The Tyrant's Bride-to-be",
    "15000793": "Kiss, Only For Study!",
    "15000794": "A Love Good for Nothing",
    "15000826": "Today I Divorce My Superstar Husband",
}


def get_drama_name(drama_id: str) -> str:
    """根据 drama_id 获取 drama_name"""
    if not drama_id:
        return None
    return DRAMA_MAPPING.get(str(drama_id))


def get_all_mappings() -> dict:
    """获取所有映射"""
    return DRAMA_MAPPING.copy()
