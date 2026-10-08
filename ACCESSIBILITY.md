# Accessibility

This project ships exactly one piece of user-facing UI: the browsable HTML
viewer that `db.php` renders for each table (chunk pages, the WAL page, and
the table index). The storage engine itself has no interface. This document
states what the viewer does well, where it deliberately falls short, and how
to report a barrier you hit.

## What works today

- Every page declares `<html lang="en">` and a per-page `<title>`.
- Row data lives in semantic `<table>` / `<thead>` / `<th>` / `<td>` markup,
  so screen readers announce it as a real table. Each cell also carries a
  `data-column` attribute naming its column.
- Navigation is plain `<a>` links, fully keyboard operable with the browser's
  default tab order. There are no custom widgets and no focus traps.
- Primary body text (green `#00ff41` on near-black `#0a0a0a`) has a contrast
  ratio of about 14:1, and secondary text (`#00aa2a`) about 6.4:1. Both are
  above the WCAG 2.1 AA threshold of 4.5:1.
- All content is real text: no images of text, no canvas rendering. Copy,
  find-in-page, and browser zoom behave normally.

## Known limitations

The viewer leans into a retro terminal aesthetic, and some of those choices
trade accessibility for the look. They are intentional, not oversights:

- The page heading carries a blinking block cursor (`blink`, 1s, infinite).
  There is no `prefers-reduced-motion` guard yet, so the motion cannot be
  disabled from within the page.
- A fixed scanline overlay (dark stripes at 15% opacity) sits above the
  content and slightly lowers effective contrast.
- The green glow (`text-shadow`) can soften glyph edges for some low-vision
  readers.
- Table cells truncate at 300px with an ellipsis and no tooltip, so long
  values are visually cut with no in-page way to reveal the full text. The
  underlying chunk file still holds the complete value.
- Footer and disabled-navigation text (`#444`, `#333`) fall below the AA
  contrast threshold.
- There is no light theme and no `prefers-color-scheme` support; the dark
  palette is the only option.
- Base font size is 13px. Browser zoom works, but there is no in-page
  control.
- There is no skip-to-content link and no explicit `scope="col"` on header
  cells.

## Reporting a barrier

If you hit an accessibility barrier in the viewer, open an issue from the
Issues tab using the Bug report template. Include the page you were on (for
example a chunk page or the WAL page), your assistive technology if any, and
what you expected to happen. Barriers that do not depend on the retro theme
(missing `scope`, no reduced-motion guard, truncation without a reveal) are
treated as ordinary bugs and fixed.
