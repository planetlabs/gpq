import planetConfig from 'eslint-config-planet';

export default [
  ...planetConfig,
  {
    ignores: ['wasm_exec.js'],
  },
  {
    languageOptions: {
      ecmaVersion: 'latest',
      globals: {
        Go: 'readonly',
      },
    },
    rules: {
      'import/no-unresolved': [
        'error',
        {
          ignore: ['^https?://'],
        },
      ],
    },
  },
];
