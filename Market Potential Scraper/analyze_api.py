import json

# 分析 searchCreative API 响应
with open('debug_api_response_20251225_191403.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

print("=== searchCreative API (页面加载时) ===")
print("StatusCode:", data.get('statusCode'))
content = data.get('content', [])
print("Content type:", type(content))

if isinstance(content, list):
    print("Content length:", len(content))
    if content:
        print("First item keys:", list(content[0].keys())[:10])
else:
    print("Content keys:", list(content.keys()))
    searchList = content.get('searchList', [])
    print("SearchList length:", len(searchList))
    if searchList:
        print("First item keys:", list(searchList[0].keys())[:15])
        print("\nFirst item sample:")
        item = searchList[0]
        for key in ['exposureNum', 'firstSeen', 'lastSeen', 'publisher', 'media', 'countries']:
            if key in item:
                print(f"  {key}: {item[key]}")

print("\n" + "="*60)
print("现在分析 materialQuickSearch API (搜索时)")
print("="*60)
