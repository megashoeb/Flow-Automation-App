/**
 * G-Labs Helper — Persona System (Phase 3)
 *
 * Each account is assigned a persona (one of 10) at setup.
 * Persona drives what the account searches for, watches on YouTube,
 * and explores on Maps — making the behavior look like a real human
 * with consistent interests.
 *
 * Variables like {city}, {food}, {brand} are filled from pools at
 * query generation time, so 50 templates × 5-10 variables per slot
 * = hundreds of unique queries per persona.
 */

// ─── Shared variable pools ───
const POOL = {
  city: [
    "Delhi", "Mumbai", "Bengaluru", "Pune", "Chennai", "Hyderabad",
    "Kolkata", "Ahmedabad", "Jaipur", "Lucknow", "Chandigarh", "Goa",
    "Indore", "Noida", "Gurgaon", "Thane",
  ],
  area: [
    "near me", "near by", "in city center", "around here",
  ],
  cuisine: [
    "Italian", "Chinese", "Indian", "Thai", "Mexican", "Japanese",
    "South Indian", "North Indian", "Punjabi", "Mughlai", "Continental",
  ],
  food_item: [
    "pizza", "biryani", "pasta", "sushi", "tacos", "burger",
    "dosa", "paneer", "butter chicken", "pav bhaav", "samosa",
    "chole bhature", "rogan josh", "dim sum", "momos", "shawarma",
  ],
  brand_food: [
    "KFC", "McDonald's", "Domino's", "Burger King", "Pizza Hut",
    "Subway", "Starbucks", "CCD", "Barista",
  ],
  brand_shop: [
    "Zara", "H&M", "Uniqlo", "Decathlon", "IKEA", "Nike", "Adidas",
    "Puma", "Levi's", "Marks & Spencer",
  ],
  car_brand: [
    "Tata", "Mahindra", "Maruti", "Hyundai", "Honda", "Toyota",
    "Kia", "MG", "Skoda", "Volkswagen", "BMW", "Mercedes",
  ],
  car_model: [
    "Nexon", "Punch", "Swift", "Creta", "Seltos", "Scorpio",
    "Brezza", "Venue", "i20", "Baleno", "XUV700", "Fortuner",
  ],
  phone_brand: [
    "iPhone", "Samsung", "OnePlus", "Redmi", "Realme", "Vivo",
    "Oppo", "Nothing", "Motorola", "Poco",
  ],
  actor: [
    "Shahrukh Khan", "Salman Khan", "Aamir Khan", "Ranbir Kapoor",
    "Hrithik Roshan", "Ranveer Singh", "Deepika Padukone", "Alia Bhatt",
    "Katrina Kaif", "Vicky Kaushal",
  ],
  cricket_player: [
    "Virat Kohli", "Rohit Sharma", "MS Dhoni", "Hardik Pandya",
    "Jasprit Bumrah", "KL Rahul", "Shubman Gill", "Rishabh Pant",
    "Suryakumar Yadav", "Bumrah",
  ],
  ipl_team: [
    "CSK", "RCB", "MI", "KKR", "SRH", "DC", "RR", "PBKS", "GT", "LSG",
  ],
  workout: [
    "chest workout", "back exercises", "leg day", "yoga for beginners",
    "abs workout", "shoulder workout", "cardio routine", "HIIT workout",
    "bicep curls", "squat form",
  ],
  recipe_dish: [
    "paneer butter masala", "dal makhani", "chicken biryani",
    "gulab jamun", "rasmalai", "kadhai paneer", "butter chicken",
    "chole bhature", "idli sambhar", "masala dosa", "palak paneer",
  ],
  destination: [
    "Goa", "Manali", "Shimla", "Darjeeling", "Kashmir", "Kerala",
    "Rajasthan", "Ladakh", "Dubai", "Singapore", "Thailand", "Bali",
  ],
  place_type: [
    "restaurants", "cafes", "shopping malls", "parks", "gyms",
    "movie theaters", "hotels", "tourist spots", "beaches",
    "temples", "historical places",
  ],
  game: [
    "BGMI", "Free Fire", "Call of Duty Mobile", "Valorant", "CS2",
    "GTA V", "FIFA 25", "Minecraft", "Fortnite", "Clash of Clans",
  ],
  study_topic: [
    "python tutorial", "javascript basics", "data structures",
    "machine learning", "UPSC preparation", "JEE physics",
    "english grammar", "algebra class 10", "calculus",
  ],
  clothing: [
    "kurta", "jeans", "t-shirt", "saree", "lehenga", "shirt",
    "suit", "dress", "jacket", "blazer",
  ],
  travel_query: [
    "flight tickets", "train booking", "hotel booking",
    "travel packages", "visa requirements", "best places to visit",
  ],
  news_topic: [
    "stock market", "breaking news", "political news", "weather",
    "current affairs", "world news", "sports news", "tech news",
  ],
};

// Helper: fill template with random pool values
function fillTemplate(template) {
  return template.replace(/\{(\w+)\}/g, (m, key) => {
    const pool = POOL[key];
    if (!pool) return m;
    return pool[Math.floor(Math.random() * pool.length)];
  });
}

// ─── Persona definitions ───
// Each persona: id, name, search templates, YouTube templates, Maps templates.
const PERSONAS = {
  foodie: {
    id: "foodie",
    name: "Foodie",
    searchTemplates: [
      "best {cuisine} restaurant {city}", "{food_item} recipe",
      "how to make {recipe_dish}", "{brand_food} menu {city}",
      "food delivery {area}", "top restaurants {city}",
      "{food_item} near me", "{cuisine} food near me",
      "best {food_item} in India", "{recipe_dish} ingredients",
      "homemade {food_item}", "{cuisine} buffet {city}",
      "best chef restaurant {city}", "street food {city}",
      "fine dining {city}", "{food_item} vs {food_item}",
      "healthy {food_item} recipe", "quick {cuisine} recipe",
      "calories in {food_item}", "restaurant deals {city}",
      "food festival {city}", "late night food {city}",
    ],
    youtubeTemplates: [
      "{recipe_dish} recipe", "how to cook {food_item}",
      "{cuisine} cooking tutorial", "street food {city}",
      "food review {brand_food}", "cooking at home",
      "{food_item} making", "chef cooking {cuisine}",
      "food vlog {city}", "restaurant review",
      "5 minute {food_item}", "desi cooking",
    ],
    mapsTemplates: [
      "{cuisine} restaurant {area}", "{brand_food} near me",
      "{food_item} restaurant", "cafe near me",
      "food court", "best restaurants", "pizza delivery",
    ],
  },

  tech_geek: {
    id: "tech_geek",
    name: "Tech Geek",
    searchTemplates: [
      "{phone_brand} new launch 2026", "{phone_brand} price {city}",
      "{phone_brand} vs {phone_brand}", "best phone under 30000",
      "laptop deals 2026", "gaming laptop review",
      "{phone_brand} camera review", "android 15 features",
      "ios vs android", "best wireless earbuds",
      "{phone_brand} specifications", "smartphone trends 2026",
      "tech news today", "ai tools 2026",
      "chatgpt vs gemini", "chrome extension", "vpn review",
      "best smartwatch 2026", "{phone_brand} update",
      "wireless charging", "5G phones", "foldable phone",
    ],
    youtubeTemplates: [
      "{phone_brand} unboxing", "{phone_brand} review",
      "tech news weekly", "laptop vs laptop",
      "{phone_brand} camera test", "tech tutorial",
      "android tips and tricks", "best apps 2026",
      "smartphone comparison", "pc build guide",
    ],
    mapsTemplates: [
      "{phone_brand} store", "electronics store near me",
      "Croma near me", "Reliance Digital", "mobile repair shop",
    ],
  },

  cricket_fan: {
    id: "cricket_fan",
    name: "Cricket Fan",
    searchTemplates: [
      "{cricket_player} score today", "{ipl_team} match",
      "IPL 2026 schedule", "{cricket_player} stats",
      "cricket news today", "{ipl_team} vs {ipl_team}",
      "live cricket score", "cricket world cup 2026",
      "{cricket_player} interview", "T20 ranking",
      "India cricket team", "{ipl_team} squad 2026",
      "test cricket score", "cricket records",
      "{cricket_player} century", "IPL auction 2026",
      "cricket highlights today", "{cricket_player} biography",
      "cricket ticket booking", "cricket stadium {city}",
    ],
    youtubeTemplates: [
      "{cricket_player} best innings", "IPL highlights",
      "{ipl_team} highlights", "cricket analysis",
      "{cricket_player} interview", "cricket funny moments",
      "match highlights today", "best catches cricket",
      "{cricket_player} six", "cricket commentary",
    ],
    mapsTemplates: [
      "cricket stadium {city}", "sports shop near me",
      "cricket academy", "nets practice {city}",
    ],
  },

  movie_buff: {
    id: "movie_buff",
    name: "Movie Buff",
    searchTemplates: [
      "{actor} new movie", "bollywood news today",
      "{actor} latest movie", "upcoming movies 2026",
      "{actor} biography", "box office collection",
      "netflix new release", "best movies 2026",
      "{actor} vs {actor}", "movie review",
      "south indian movies", "hollywood new release",
      "{actor} awards", "web series recommendation",
      "oscar nominations", "film festival 2026",
      "{actor} family", "amazon prime new",
      "disney plus new", "best director",
      "actor net worth", "movie songs 2026",
    ],
    youtubeTemplates: [
      "{actor} interview", "movie trailer 2026",
      "bollywood news", "film review",
      "{actor} new song", "behind the scenes",
      "movie reaction video", "best movie scenes",
      "{actor} funny moments", "bollywood gossip",
    ],
    mapsTemplates: [
      "PVR cinema {city}", "movie theater near me",
      "INOX {city}", "multiplex near me", "cinema hall",
    ],
  },

  fitness: {
    id: "fitness",
    name: "Fitness Enthusiast",
    searchTemplates: [
      "{workout} for beginners", "gym workout plan",
      "protein diet", "weight loss tips",
      "{workout} at home", "best protein powder",
      "muscle gain diet", "calories burned {workout}",
      "home workout routine", "morning exercise",
      "yoga poses", "meditation for stress",
      "healthy breakfast", "vegetarian protein sources",
      "fat loss exercise", "abs workout home",
      "pre workout meal", "post workout snack",
      "gym near me", "personal trainer",
    ],
    youtubeTemplates: [
      "{workout} tutorial", "home workout no equipment",
      "yoga morning routine", "fitness motivation",
      "gym workout video", "abs in 10 minutes",
      "healthy recipe", "30 day challenge",
      "full body workout", "protein shake recipe",
    ],
    mapsTemplates: [
      "gym {area}", "yoga classes {city}",
      "fitness center near me", "park for running",
      "swimming pool {city}",
    ],
  },

  car_enthusiast: {
    id: "car_enthusiast",
    name: "Car Enthusiast",
    searchTemplates: [
      "{car_brand} {car_model} price", "{car_model} review",
      "{car_brand} new launch 2026", "{car_model} vs {car_model}",
      "best SUV under 20 lakh", "electric car india",
      "{car_brand} service center", "car insurance",
      "{car_model} mileage", "{car_brand} showroom {city}",
      "car loan calculator", "best hatchback 2026",
      "luxury cars india", "{car_model} features",
      "car modifications", "road trip india",
      "car service cost", "best car for family",
      "automatic vs manual", "tyre brand comparison",
    ],
    youtubeTemplates: [
      "{car_model} review", "{car_brand} vs {car_brand}",
      "car test drive", "auto expo 2026",
      "car modification", "supercar india",
      "{car_model} test", "car maintenance tips",
      "new launch review", "best cars 2026",
    ],
    mapsTemplates: [
      "{car_brand} showroom {city}", "car service center",
      "petrol pump near me", "car wash {area}",
      "Maruti dealer", "car accessories shop",
    ],
  },

  fashion: {
    id: "fashion",
    name: "Fashion Lover",
    searchTemplates: [
      "{brand_shop} sale", "latest {clothing} trends",
      "{clothing} for women", "{clothing} for men",
      "{brand_shop} {city}", "online shopping offers",
      "wedding {clothing}", "party wear",
      "summer collection 2026", "winter jacket",
      "designer {clothing}", "branded shoes",
      "makeup tutorial", "skincare routine",
      "hair care tips", "nail art",
      "fashion trends 2026", "budget fashion",
      "ethnic wear {city}", "accessory trends",
    ],
    youtubeTemplates: [
      "fashion haul", "{clothing} styling",
      "makeup tutorial", "outfit of the day",
      "{brand_shop} haul", "styling tips",
      "seasonal fashion", "budget styling",
      "skincare routine", "grwm",
    ],
    mapsTemplates: [
      "{brand_shop} {city}", "shopping mall near me",
      "clothing store", "fashion boutique",
      "saree shop {area}", "branded store",
    ],
  },

  traveler: {
    id: "traveler",
    name: "Traveler",
    searchTemplates: [
      "{destination} tourist places", "{destination} hotels",
      "flight to {destination}", "{travel_query}",
      "best time to visit {destination}", "{destination} packages",
      "{destination} food", "visa for {destination}",
      "budget trip to {destination}", "train to {destination}",
      "solo travel {destination}", "adventure activities {destination}",
      "{destination} weather", "things to do in {destination}",
      "airbnb {destination}", "homestay {destination}",
      "backpacking {destination}", "travel insurance",
      "{destination} itinerary", "road trip to {destination}",
    ],
    youtubeTemplates: [
      "{destination} vlog", "travel guide {destination}",
      "india travel vlog", "flight review",
      "hotel tour", "{destination} food tour",
      "budget travel", "top 10 places to visit",
      "{destination} drone view", "travel tips",
    ],
    mapsTemplates: [
      "tourist attractions {destination}", "airport {city}",
      "railway station {city}", "hotels {destination}",
      "travel agency {city}",
    ],
  },

  gamer: {
    id: "gamer",
    name: "Gamer",
    searchTemplates: [
      "{game} tips", "{game} new update",
      "gaming pc build", "{game} vs {game}",
      "best gaming mouse", "gaming laptop review",
      "{game} mobile download", "esports india",
      "{game} tournament", "gaming chair review",
      "{game} walkthrough", "gaming headset",
      "console vs pc gaming", "{game} free download",
      "{game} cheats", "best rpg games 2026",
      "indie games", "gaming peripherals",
      "{game} latest news", "streamer setup",
    ],
    youtubeTemplates: [
      "{game} gameplay", "{game} walkthrough",
      "gaming stream", "{game} highlights",
      "gaming setup tour", "{game} tips and tricks",
      "pc build 2026", "gaming moments funny",
      "{game} tutorial", "pro player gameplay",
    ],
    mapsTemplates: [
      "gaming cafe {city}", "pc components store",
      "esports arena", "gaming zone {area}",
    ],
  },

  student: {
    id: "student",
    name: "Student",
    searchTemplates: [
      "{study_topic}", "how to study effectively",
      "{study_topic} notes", "coaching institute {city}",
      "exam preparation tips", "time management study",
      "scholarship india", "college admissions",
      "{study_topic} examples", "online courses free",
      "competitive exam syllabus", "question bank",
      "sample papers", "previous year questions",
      "career guidance", "best university india",
      "{study_topic} tutorial", "study hacks",
      "entrance exam dates", "books for {study_topic}",
    ],
    youtubeTemplates: [
      "{study_topic} lecture", "study with me",
      "{study_topic} explained", "topper interview",
      "study motivation", "exam tips",
      "{study_topic} crash course", "homework help",
      "revision techniques", "toppers strategy",
    ],
    mapsTemplates: [
      "library {city}", "coaching center {area}",
      "bookstore near me", "cafe for studying",
      "college {city}", "xerox shop",
    ],
  },
};

const PERSONA_LIST = Object.keys(PERSONAS);

// Assign a persona to an account (stable — same account always gets same persona).
// Uses email hash so assignment is deterministic.
function personaForAccount(email) {
  if (!email) return PERSONAS.foodie;
  let hash = 0;
  for (let i = 0; i < email.length; i++) {
    hash = ((hash << 5) - hash) + email.charCodeAt(i);
    hash |= 0;
  }
  const idx = Math.abs(hash) % PERSONA_LIST.length;
  return PERSONAS[PERSONA_LIST[idx]];
}

// Pick a random search query for a persona (fills variables).
function personaSearchQuery(persona) {
  // 10% chance: random off-topic query from another persona (surprise factor)
  if (Math.random() < 0.1) {
    const alt = PERSONAS[PERSONA_LIST[Math.floor(Math.random() * PERSONA_LIST.length)]];
    return fillTemplate(alt.searchTemplates[Math.floor(Math.random() * alt.searchTemplates.length)]);
  }
  const tpl = persona.searchTemplates[Math.floor(Math.random() * persona.searchTemplates.length)];
  return fillTemplate(tpl);
}

function personaYoutubeQuery(persona) {
  const tpl = persona.youtubeTemplates[Math.floor(Math.random() * persona.youtubeTemplates.length)];
  return fillTemplate(tpl);
}

function personaMapsQuery(persona) {
  const tpl = persona.mapsTemplates[Math.floor(Math.random() * persona.mapsTemplates.length)];
  return fillTemplate(tpl);
}

// Expose globals for background.js (service worker context)
self.PERSONAS = PERSONAS;
self.personaForAccount = personaForAccount;
self.personaSearchQuery = personaSearchQuery;
self.personaYoutubeQuery = personaYoutubeQuery;
self.personaMapsQuery = personaMapsQuery;
