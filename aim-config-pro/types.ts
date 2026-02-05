export enum Season {
  AllSeason = "AllSeason",
  Winter = "Winter",
  Summer = "Summer",
  None = ""
}

export enum RunMode {
  PerSegment = "PER_SEGMENT",
  Global = "GLOBAL"
}

export interface NotebookParams {
  RUN_MODE: RunMode;
  TOTAL_PER_SEGMENT: number;
  TOTAL_OVERALL: number;
  BATCH_SIZE: number;
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
