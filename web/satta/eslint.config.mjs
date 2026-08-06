import { defineConfig, globalIgnores } from "eslint/config";
import nextVitals from "eslint-config-next/core-web-vitals";
import nextTs from "eslint-config-next/typescript";

const eslintConfig = defineConfig([
  ...nextVitals,
  ...nextTs,
  // Override default ignores of eslint-config-next.
  globalIgnores([
    // Default ignores of eslint-config-next:
    ".next/**",
    "out/**",
    "build/**",
    "next-env.d.ts",
    "components/charts/**",
    "components/evilcharts/**",
    "components/kibo-ui/**",
    "components/lytenyte-core.tsx",
    "components/stats-01.tsx",
    "components/table-02.tsx",
    "components/shimmering-text.tsx",
    "components/ui/carousel.tsx",
    "hooks/**",
  ]),
]);

export default eslintConfig;
