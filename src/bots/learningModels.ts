export type State = {
  k1: number;
  k2: number;
  k3: number;
  k4: number
}

export interface LearningModel {
  updateModel(state: State, action: number, reward: number): void;
  getOptimalAction(state: string): number;
}
