tests:
  - description: "async for loop fn"
    before: |
      async def func_name():
        async for i in range(10):
            await routine()
        return 42
    transformers: [AsyncToSync]
    after: |
      def func_name():
        for i in range(10):
            routine()
        return 42
