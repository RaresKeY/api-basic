# Model Tool-Calling Evaluation

Generated: 2026-04-22 19:58:06

## Scenario

Copied `examples/shop-example.db`, asked the model to record electronics sales, compute monthly stats, and plan a NovaTech bulk buy.

Score is out of 10: inventory-first flow 2, correct sales 3, stats 1, plan 1, final response 2, no leaked narration 1.

## Tested Models

| Model | Score | Calls | Comments |
|---|---:|---|---|
| `models/gemini-2.0-flash-lite-001` | 0/10 | none | API error: 429 RESOURCE_EXHAUSTED. {'error': {'code': 429, 'message': 'You exceeded your current quota, please check your plan and billing details. For more information on this error, head to |
| `models/gemini-2.0-flash-lite` | 0/10 | none | API error: 429 RESOURCE_EXHAUSTED. {'error': {'code': 429, 'message': 'You exceeded your current quota, please check your plan and billing details. For more information on this error, head to |
| `models/gemma-4-26b-a4b-it` | 9/10 | fetch_inventory, record_sales, get_sales_stats, plan_partner_order | fetched inventory before recording sales; sales quantities correct; final text leaked planning narration |
| `models/gemma-4-31b-it` | 10/10 | fetch_inventory, record_sales, get_sales_stats, plan_partner_order | fetched inventory before recording sales; sales quantities correct; clean final response |
| `models/gemini-flash-lite-latest` | 10/10 | fetch_inventory, record_sales, get_sales_stats, plan_partner_order | fetched inventory before recording sales; sales quantities correct; clean final response |
| `models/gemini-2.5-flash-lite` | 7/10 | fetch_inventory, record_sales, get_sales_stats, plan_partner_order | fetched inventory before recording sales; sales mismatch: {}; clean final response |
| `models/gemini-3.1-flash-lite-preview` | 10/10 | fetch_inventory, record_sales, get_sales_stats, plan_partner_order | fetched inventory before recording sales; sales quantities correct; clean final response |

## Selected Models

- `models/gemini-2.0-flash-lite-001` - Gemini 2.0 Flash-Lite 001
- `models/gemini-2.0-flash-lite` - Gemini 2.0 Flash-Lite
- `models/gemma-4-26b-a4b-it` - Gemma 4 26B A4B IT
- `models/gemma-4-31b-it` - Gemma 4 31B IT
- `models/gemini-flash-lite-latest` - Gemini Flash-Lite Latest
- `models/gemini-2.5-flash-lite` - Gemini 2.5 Flash-Lite
- `models/gemini-3.1-flash-lite-preview` - Gemini 3.1 Flash Lite Preview

## All Available Models

- `models/gemini-2.5-flash` - Gemini 2.5 Flash - generateContent, countTokens, createCachedContent, batchGenerateContent
- `models/gemini-2.5-pro` - Gemini 2.5 Pro - generateContent, countTokens, createCachedContent, batchGenerateContent
- `models/gemini-2.0-flash` - Gemini 2.0 Flash - generateContent, countTokens, createCachedContent, batchGenerateContent
- `models/gemini-2.0-flash-001` - Gemini 2.0 Flash 001 - generateContent, countTokens, createCachedContent, batchGenerateContent
- `models/gemini-2.0-flash-lite-001` - Gemini 2.0 Flash-Lite 001 - generateContent, countTokens, createCachedContent, batchGenerateContent
- `models/gemini-2.0-flash-lite` - Gemini 2.0 Flash-Lite - generateContent, countTokens, createCachedContent, batchGenerateContent
- `models/gemini-2.5-flash-preview-tts` - Gemini 2.5 Flash Preview TTS - countTokens, generateContent
- `models/gemini-2.5-pro-preview-tts` - Gemini 2.5 Pro Preview TTS - countTokens, generateContent, batchGenerateContent
- `models/gemma-3-1b-it` - Gemma 3 1B - generateContent, countTokens
- `models/gemma-3-4b-it` - Gemma 3 4B - generateContent, countTokens
- `models/gemma-3-12b-it` - Gemma 3 12B - generateContent, countTokens
- `models/gemma-3-27b-it` - Gemma 3 27B - generateContent, countTokens
- `models/gemma-3n-e4b-it` - Gemma 3n E4B - generateContent, countTokens
- `models/gemma-3n-e2b-it` - Gemma 3n E2B - generateContent, countTokens
- `models/gemma-4-26b-a4b-it` - Gemma 4 26B A4B IT - generateContent, countTokens
- `models/gemma-4-31b-it` - Gemma 4 31B IT - generateContent, countTokens
- `models/gemini-flash-latest` - Gemini Flash Latest - generateContent, countTokens, createCachedContent, batchGenerateContent
- `models/gemini-flash-lite-latest` - Gemini Flash-Lite Latest - generateContent, countTokens, createCachedContent, batchGenerateContent
- `models/gemini-pro-latest` - Gemini Pro Latest - generateContent, countTokens, createCachedContent, batchGenerateContent
- `models/gemini-2.5-flash-lite` - Gemini 2.5 Flash-Lite - generateContent, countTokens, createCachedContent, batchGenerateContent
- `models/gemini-2.5-flash-image` - Nano Banana - generateContent, countTokens, batchGenerateContent
- `models/gemini-3-pro-preview` - Gemini 3 Pro Preview - generateContent, countTokens, createCachedContent, batchGenerateContent
- `models/gemini-3-flash-preview` - Gemini 3 Flash Preview - generateContent, countTokens, createCachedContent, batchGenerateContent
- `models/gemini-3.1-pro-preview` - Gemini 3.1 Pro Preview - generateContent, countTokens, createCachedContent, batchGenerateContent
- `models/gemini-3.1-pro-preview-customtools` - Gemini 3.1 Pro Preview Custom Tools - generateContent, countTokens, createCachedContent, batchGenerateContent
- `models/gemini-3.1-flash-lite-preview` - Gemini 3.1 Flash Lite Preview - generateContent, countTokens, createCachedContent, batchGenerateContent
- `models/gemini-3-pro-image-preview` - Nano Banana Pro - generateContent, countTokens, batchGenerateContent
- `models/nano-banana-pro-preview` - Nano Banana Pro - generateContent, countTokens, batchGenerateContent
- `models/gemini-3.1-flash-image-preview` - Nano Banana 2 - generateContent, countTokens, batchGenerateContent
- `models/lyria-3-clip-preview` - Lyria 3 Clip Preview - generateContent, countTokens
- `models/lyria-3-pro-preview` - Lyria 3 Pro Preview - generateContent, countTokens
- `models/gemini-3.1-flash-tts-preview` - Gemini 3.1 Flash TTS Preview - generateContent, countTokens, batchGenerateContent
- `models/gemini-robotics-er-1.5-preview` - Gemini Robotics-ER 1.5 Preview - generateContent, countTokens
- `models/gemini-robotics-er-1.6-preview` - Gemini Robotics-ER 1.6 Preview - generateContent, countTokens, createCachedContent, batchGenerateContent
- `models/gemini-2.5-computer-use-preview-10-2025` - Gemini 2.5 Computer Use Preview 10-2025 - generateContent, countTokens
- `models/deep-research-max-preview-04-2026` - Deep Research Max Preview (Apr-21-2026) - generateContent, countTokens
- `models/deep-research-preview-04-2026` - Deep Research Preview (Apr-21-2026) - generateContent, countTokens
- `models/deep-research-pro-preview-12-2025` - Deep Research Pro Preview (Dec-12-2025) - generateContent, countTokens
- `models/gemini-embedding-001` - Gemini Embedding 001 - embedContent, countTextTokens, countTokens, asyncBatchEmbedContent
- `models/gemini-embedding-2-preview` - Gemini Embedding 2 Preview - embedContent, countTextTokens, countTokens, asyncBatchEmbedContent
- `models/gemini-embedding-2` - Gemini Embedding 2 - embedContent, countTextTokens, countTokens, asyncBatchEmbedContent
- `models/aqa` - Model that performs Attributed Question Answering. - generateAnswer
- `models/imagen-4.0-generate-001` - Imagen 4 - predict
- `models/imagen-4.0-ultra-generate-001` - Imagen 4 Ultra - predict
- `models/imagen-4.0-fast-generate-001` - Imagen 4 Fast - predict
- `models/veo-2.0-generate-001` - Veo 2 - predictLongRunning
- `models/veo-3.0-generate-001` - Veo 3 - predictLongRunning
- `models/veo-3.0-fast-generate-001` - Veo 3 fast - predictLongRunning
- `models/veo-3.1-generate-preview` - Veo 3.1 - predictLongRunning
- `models/veo-3.1-fast-generate-preview` - Veo 3.1 fast - predictLongRunning
- `models/veo-3.1-lite-generate-preview` - Veo 3.1 lite - predictLongRunning
- `models/gemini-2.5-flash-native-audio-latest` - Gemini 2.5 Flash Native Audio Latest - countTokens, bidiGenerateContent
- `models/gemini-2.5-flash-native-audio-preview-09-2025` - Gemini 2.5 Flash Native Audio Preview 09-2025 - countTokens, bidiGenerateContent
- `models/gemini-2.5-flash-native-audio-preview-12-2025` - Gemini 2.5 Flash Native Audio Preview 12-2025 - countTokens, bidiGenerateContent
- `models/gemini-3.1-flash-live-preview` - Gemini 3.1 Flash Live Preview - bidiGenerateContent
