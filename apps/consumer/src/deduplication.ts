/**
 * Sliding Window Deduplication Engine
 * Uses bounded Map with FIFO/LRU eviction to guarantee O(1) memory complexity.
 */

export class DeduplicationStore {
  private readonly cache: Map<string, boolean>;
  private readonly maxCapacity: number;

  constructor(maxCapacity = 500000) {
    this.maxCapacity = Math.max(1, maxCapacity);
    this.cache = new Map<string, boolean>();
  }

  /**
   * Checks whether the given messageId has already been recorded.
   */
  public isDuplicate(messageId: string): boolean {
    return this.cache.has(messageId);
  }

  /**
   * Records a messageId in the store.
   * Evicts the oldest entry if max capacity is reached.
   */
  public record(messageId: string): void {
    if (this.cache.has(messageId)) {
      // Re-insert to refresh recency
      this.cache.delete(messageId);
      this.cache.set(messageId, true);
      return;
    }

    if (this.cache.size >= this.maxCapacity) {
      const oldestKey = this.cache.keys().next().value;
      if (oldestKey !== undefined) {
        this.cache.delete(oldestKey);
      }
    }

    this.cache.set(messageId, true);
  }

  /**
   * Clears all tracked keys from the store.
   */
  public clear(): void {
    this.cache.clear();
  }

  /**
   * Returns current count of tracked keys.
   */
  public size(): number {
    return this.cache.size;
  }
}
