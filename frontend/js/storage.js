/**
 * TransferStorage — IndexedDB wrapper for persistent chunk caching and resume.
 *
 * Stores:
 *   1. "transfers" store: metadata (transferId, fileName, fileSize, totalChunks, sha256, status, etc.)
 *   2. "chunks" store: chunk binary blobs keyed by [transferId, chunkIndex]
 *
 * This allows transfers to:
 *   - Pause and resume without losing previously transferred chunks
 *   - Survive network dropouts or browser refreshes
 *   - Prevent excessive RAM consumption during large file transfers
 */

const DB_NAME = 'p2p_mesh_transfers';
const DB_VERSION = 1;
const STORE_TRANSFERS = 'transfers';
const STORE_CHUNKS = 'chunks';

export class TransferStorage {
  constructor() {
    this.db = null;
    this._initPromise = this._openDB();
  }

  async _openDB() {
    if (typeof indexedDB === 'undefined') {
      console.warn('[Storage] IndexedDB not supported in this environment.');
      return null;
    }

    return new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, DB_VERSION);

      req.onupgradeneeded = (e) => {
        const db = e.target.result;

        // Store for transfer metadata
        if (!db.objectStoreNames.contains(STORE_TRANSFERS)) {
          const tStore = db.createObjectStore(STORE_TRANSFERS, { keyPath: 'transferId' });
          tStore.createIndex('status', 'status', { unique: false });
          tStore.createIndex('updatedAt', 'updatedAt', { unique: false });
        }

        // Store for binary chunks: composite key [transferId, chunkIndex]
        if (!db.objectStoreNames.contains(STORE_CHUNKS)) {
          const cStore = db.createObjectStore(STORE_CHUNKS, { keyPath: ['transferId', 'chunkIndex'] });
          cStore.createIndex('transferId', 'transferId', { unique: false });
        }
      };

      req.onsuccess = () => {
        this.db = req.result;
        resolve(this.db);
      };

      req.onerror = () => {
        console.error('[Storage] IndexedDB open error:', req.error);
        reject(req.error);
      };
    });
  }

  async ready() {
    await this._initPromise;
    return !!this.db;
  }

  /**
   * Save or update transfer metadata
   */
  async saveTransferMeta(meta) {
    if (!await this.ready()) return;
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction([STORE_TRANSFERS], 'readwrite');
      const store = tx.objectStore(STORE_TRANSFERS);
      const record = {
        ...meta,
        updatedAt: Date.now(),
      };
      const req = store.put(record);
      req.onsuccess = () => resolve(record);
      req.onerror = () => reject(req.error);
    });
  }

  /**
   * Get transfer metadata by transferId
   */
  async getTransferMeta(transferId) {
    if (!await this.ready()) return null;
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction([STORE_TRANSFERS], 'readonly');
      const store = tx.objectStore(STORE_TRANSFERS);
      const req = store.get(transferId);
      req.onsuccess = () => resolve(req.result || null);
      req.onerror = () => reject(req.error);
    });
  }

  /**
   * Update status of a transfer (e.g., 'active', 'paused', 'completed')
   */
  async updateTransferStatus(transferId, status) {
    const meta = await this.getTransferMeta(transferId);
    if (!meta) return null;
    meta.status = status;
    return this.saveTransferMeta(meta);
  }

  /**
   * Save an individual chunk payload (ArrayBuffer) into IndexedDB
   */
  async saveChunk(transferId, chunkIndex, arrayBuffer) {
    if (!await this.ready()) return;
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction([STORE_CHUNKS], 'readwrite');
      const store = tx.objectStore(STORE_CHUNKS);
      const record = {
        transferId,
        chunkIndex,
        data: arrayBuffer,
        size: arrayBuffer.byteLength,
        savedAt: Date.now(),
      };
      const req = store.put(record);
      req.onsuccess = () => resolve(true);
      req.onerror = () => reject(req.error);
    });
  }

  /**
   * Get all stored chunk indices for a given transfer
   * Returns a Set of number indices
   */
  async getStoredChunkIndices(transferId) {
    if (!await this.ready()) return new Set();
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction([STORE_CHUNKS], 'readonly');
      const store = tx.objectStore(STORE_CHUNKS);
      const index = store.index('transferId');
      const req = index.getAllKeys(IDBKeyRange.only(transferId));

      req.onsuccess = () => {
        // composite keys are [transferId, chunkIndex]
        const set = new Set();
        for (const key of req.result) {
          if (Array.isArray(key)) set.add(key[1]);
        }
        resolve(set);
      };
      req.onerror = () => reject(req.error);
    });
  }

  /**
   * Get a single chunk's ArrayBuffer
   */
  async getChunk(transferId, chunkIndex) {
    if (!await this.ready()) return null;
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction([STORE_CHUNKS], 'readonly');
      const store = tx.objectStore(STORE_CHUNKS);
      const req = store.get([transferId, chunkIndex]);
      req.onsuccess = () => resolve(req.result ? req.result.data : null);
      req.onerror = () => reject(req.error);
    });
  }

  /**
   * Retrieve all chunks in order [0..totalChunks-1] for final assembly
   */
  async getAllChunks(transferId, totalChunks) {
    if (!await this.ready()) return [];
    const chunks = new Array(totalChunks);
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction([STORE_CHUNKS], 'readonly');
      const store = tx.objectStore(STORE_CHUNKS);
      const index = store.index('transferId');
      const req = index.openCursor(IDBKeyRange.only(transferId));

      req.onsuccess = (e) => {
        const cursor = e.target.result;
        if (cursor) {
          const chunk = cursor.value;
          if (chunk && typeof chunk.chunkIndex === 'number' && chunk.chunkIndex < totalChunks) {
            chunks[chunk.chunkIndex] = chunk.data;
          }
          cursor.continue();
        } else {
          resolve(chunks);
        }
      };
      req.onerror = () => reject(req.error);
    });
  }

  /**
   * Delete transfer metadata and all associated chunks
   */
  async deleteTransfer(transferId) {
    if (!await this.ready()) return;
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction([STORE_TRANSFERS, STORE_CHUNKS], 'readwrite');
      const tStore = tx.objectStore(STORE_TRANSFERS);
      const cStore = tx.objectStore(STORE_CHUNKS);

      tStore.delete(transferId);

      const index = cStore.index('transferId');
      const req = index.openKeyCursor(IDBKeyRange.only(transferId));
      req.onsuccess = (e) => {
        const cursor = e.target.result;
        if (cursor) {
          cStore.delete(cursor.primaryKey);
          cursor.continue();
        }
      };

      tx.oncomplete = () => resolve(true);
      tx.onerror = () => reject(tx.error);
    });
  }

  /**
   * Get list of all non-completed transfers (for pause/resume list or stats)
   */
  async listIncompleteTransfers() {
    if (!await this.ready()) return [];
    return new Promise((resolve, reject) => {
      const tx = this.db.transaction([STORE_TRANSFERS], 'readonly');
      const store = tx.objectStore(STORE_TRANSFERS);
      const req = store.getAll();
      req.onsuccess = () => {
        const pending = (req.result || []).filter(t => t.status !== 'completed');
        resolve(pending);
      };
      req.onerror = () => reject(req.error);
    });
  }
}
