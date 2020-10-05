from typing import List, Tuple, TypedDict
import pandas as pd

class IresultFormat(TypedDict):
    demandDf : pd.core.frame.DataFrame
    # (currDate,entity,purityPercent )
    purityPercent : Tuple

class Idemand_purity_dict(TypedDict):
    data: List[Tuple]
    purityPercentage: List[Tuple]