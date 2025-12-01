export enum Season {
  AllSeason = "AllSeason",
  Winter = "Winter",
  Summer = "Summer",
  None = ""
}

export interface NotebookParams {
  TOTAL_PER_SEGMENT: number;
  GOLDILOCKS_ZONE_PCT: number;
  PRICE_FLUCTUATION_UPPER: number;
  PRICE_FLUCTUATION_LOWER: number;
  BRAND_ENHANCER: string;
  MODEL_ENHANCER: string;
  SEASON: Season;
  LIMIT_TO_SEGMENTS: string;
}

export interface NotebookOutput {
  success: boolean;
  message: string;
  timestamp: string;
}
