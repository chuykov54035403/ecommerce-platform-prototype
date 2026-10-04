from decimal import Decimal


PRODUCTS = [
    (
        "11111111-1111-1111-1111-111111111111",
        "Palit GeForce RTX 5060 Dual 8GB",
        "GPU",
        "Palit",
        Decimal("329.00"),
        "Compact RTX GPU for gaming PCs",
        {"chip": "RTX 5060", "memory_gb": 8, "memory_type": "GDDR7"},
    ),
    (
        "11111111-1111-1111-1111-111111111112",
        "ASUS GeForce RTX 5070 Prime 12GB",
        "GPU",
        "ASUS",
        Decimal("579.00"),
        "High performance NVIDIA graphics card",
        {"chip": "RTX 5070", "memory_gb": 12, "memory_type": "GDDR7"},
    ),
    (
        "22222222-2222-2222-2222-222222222221",
        "AMD Ryzen 7 9700X",
        "CPU",
        "AMD",
        Decimal("349.00"),
        "AM5 desktop processor",
        {"socket": "AM5", "cores": 8, "threads": 16},
    ),
    (
        "22222222-2222-2222-2222-222222222222",
        "Intel Core i5-14600K",
        "CPU",
        "Intel",
        Decimal("289.00"),
        "LGA1700 desktop processor",
        {"socket": "LGA1700", "cores": 14, "threads": 20},
    ),
    (
        "33333333-3333-3333-3333-333333333331",
        "Samsung 990 EVO Plus 1TB",
        "SSD",
        "Samsung",
        Decimal("109.00"),
        "Fast NVMe SSD",
        {"capacity_gb": 1000, "interface": "PCIe 4.0", "form_factor": "M.2"},
    ),
    (
        "33333333-3333-3333-3333-333333333332",
        "Kingston NV3 500GB",
        "SSD",
        "Kingston",
        Decimal("45.00"),
        "Budget NVMe SSD with limited demo stock",
        {"capacity_gb": 500, "interface": "PCIe 4.0", "form_factor": "M.2"},
    ),
]
