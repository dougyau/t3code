export const frameBytes: number;
export function frame(value: unknown, label?: string): string;
export function frames(receive: (value: unknown) => void, options?: { label?: string; fatalUtf8?: boolean }): (chunk: Buffer) => void;
