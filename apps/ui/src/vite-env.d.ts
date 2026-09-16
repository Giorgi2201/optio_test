/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Optional absolute base URL for the pipeline API. Defaults to relative paths (Vite proxy). */
  readonly VITE_API_BASE_URL?: string;
  /** Display-only label for the pipeline daemon port shown in the header badge. */
  readonly VITE_PIPELINE_PORT?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
