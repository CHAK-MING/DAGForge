function normalizeSearchText(value: string): string {
  return value
    .toLowerCase()
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function matchesSearchQuery(
  query: string,
  ...candidates: Array<string | null | undefined>
): boolean {
  const rawQuery = query.trim();
  if (rawQuery === "") {
    return true;
  }

  const normalizedQuery = normalizeSearchText(rawQuery);

  return candidates.some((candidate) => {
    if (!candidate) {
      return false;
    }

    const rawCandidate = candidate.toLowerCase();
    const normalizedCandidate = normalizeSearchText(candidate);

    return (
      rawCandidate.includes(rawQuery.toLowerCase()) ||
      normalizedCandidate.includes(normalizedQuery)
    );
  });
}
