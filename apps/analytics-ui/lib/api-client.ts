import { API_BASE_URL } from "./utils";

export async function fetchJson<T>(
  endpoint: string,
  init?: RequestInit
): Promise<T> {
  const url = `${API_BASE_URL}${endpoint}`;
  const response = await fetch(url, {
    ...init,
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      ...(init?.headers ?? {})
    }
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(
      `Analytics API request failed (${response.status}): ${message || "unknown"}`
    );
  }

  return (await response.json()) as T;
}
