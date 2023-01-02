package ceng.ceng351.foodrecdb;


import java.sql.*;
import java.util.ArrayList;

public class FOODRECDB implements IFOODRECDB {
    private static final String user = "e2448900"; // TODO: Your userName
    private static final String password = "VeUWT0HNrfHmOLa9"; //  TODO: Your password
    private static final String host = "momcorp.ceng.metu.edu.tr"; // host name
    private static final String database = "db2448900"; // TODO: Your database name
    private static final int port = 8080; // port


    private static DatabaseMetaData dbm;
    private static Connection connection = null;
    private static Statement st = null;

    public static void connect() {

        String url = "jdbc:mysql://" + host + ":" + port + "/" + database;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection =  DriverManager.getConnection(url, user, password);
        }
        catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    public static void disconnect() {

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    @Override
    public void initialize() {
        connect();
        try{
            st = connection.createStatement();
            dbm = connection.getMetaData();
        } catch (SQLException e){
            printException(e);
        }
    }

    @Override
    public int createTables() {
        ResultSet rs;

        String sql1 = "create table MenuItems(itemID int, itemName varchar(40), cuisine varchar(20),price int," +
                        "primary key (itemID));";

        String sql2 = "create table Ingredients(ingredientID int, ingredientName varchar(40)," +
                        "primary key (ingredientID));";

        String sql3 = "create table Includes(itemID int, ingredientID int," +
                        "primary key (itemID, ingredientID), " +
                        "foreign key (ingredientID) references Ingredients(ingredientID) on delete cascade, " +
                        "foreign key (itemID) references MenuItems(itemID) on delete cascade);";

        String sql4 = "create table Ratings(ratingID int, itemID int, rating int, ratingDate date, " +
                        "primary key (ratingID), " +
                        "foreign key (itemID) references MenuItems(itemID) on delete cascade);";

        String sql5 = "create table DietaryCategories(ingredientID int, dietaryCategory varchar(20)," +
                        "primary key (ingredientID, dietaryCategory)," +
                        "foreign key (ingredientID) references Ingredients(ingredientID) on delete cascade);";

        try {
            rs = dbm.getTables(null, null, "MenuItems", null);
            if(!rs.next())
                st.executeUpdate(sql1);

            rs = dbm.getTables(null, null, "Ingredients", null);
            if(!rs.next())
                st.executeUpdate(sql2);

            rs = dbm.getTables(null, null, "Includes", null);
            if(!rs.next())
                st.executeUpdate(sql3);

            rs = dbm.getTables(null, null, "Ratings", null);
            if(!rs.next())
                st.executeUpdate(sql4);

            rs = dbm.getTables(null, null, "DietaryCategories", null);
            if(!rs.next())
                st.executeUpdate(sql5);

        } catch (SQLException e) {
            printException(e);
        }
        return 5;
    }

    @Override
    public int dropTables() {
        ResultSet rs;

        String sql1 = "drop table MenuItems;";
        String sql2 = "drop table Ingredients;";
        String sql3 = "drop table Includes;";
        String sql4 = "drop table Ratings;";
        String sql5 = "drop table DietaryCategories;";

        try {
            rs = dbm.getTables(null, null, "Includes", null);
            if(rs.next())
                st.executeUpdate(sql3);


            rs = dbm.getTables(null, null, "DietaryCategories", null);
            if(rs.next())
                st.executeUpdate(sql5);

            rs = dbm.getTables(null, null, "Ingredients", null);
            if(rs.next())
                st.executeUpdate(sql2);

            rs = dbm.getTables(null, null, "Ratings", null);
            if(rs.next())
                st.executeUpdate(sql4);

            rs = dbm.getTables(null, null, "MenuItems", null);
            if(rs.next())
                st.executeUpdate(sql1);

        } catch (SQLException e) {
            printException(e);
        }
        return 5;
    }

    @Override
    public int insertMenuItems(MenuItem[] items) {
        int itemID;
        String itemName;
        String cuisine;
        int price;

        int count = 0;
        String sql, search_sql;
        ResultSet rs;

        for (MenuItem item : items){
            count++;
            itemID = item.getItemID();
            itemName = item.getItemName();
            cuisine = item.getCuisine();
            price = item.getPrice();

            sql = String.format("insert into MenuItems (itemID, itemName, cuisine, price) " +
                            "values (%d, \"%s\", \"%s\", %d);",
                    itemID,itemName, cuisine, price);
            try{
                search_sql = String.format("select * from MenuItems MI where MI.itemID=%d;", itemID);
                rs = st.executeQuery(search_sql);
                if(!rs.next())
                    st.executeUpdate(sql);

            } catch (SQLException e) {
                printException(e);
            }
        }
        return count;
    }

    @Override
    public int insertIngredients(Ingredient[] ingredients) {
        int ingredientID;
        String ingredientName;

        int count = 0;
        String sql, search_sql;
        ResultSet rs;

        for (Ingredient ingredient : ingredients){
            count++;
            ingredientID = ingredient.getIngredientID();
            ingredientName = ingredient.getIngredientName();
            sql = String.format("insert into Ingredients (ingredientID, ingredientName) " +
                    "values (%d, \"%s\");", ingredientID, ingredientName);
            try{
                search_sql = String.format("select * from Ingredients I where I.ingredientID=%d;", ingredientID);
                rs = st.executeQuery(search_sql);
                if(!rs.next())
                    st.executeUpdate(sql);
            } catch (SQLException e) {
                printException(e);
            }
        }
        return count;
    }

    @Override
    public int insertIncludes(Includes[] includes) {
        int itemID;
        int ingredientID;

        int count = 0;
        String sql, search_sql;
        ResultSet rs;

        for (Includes include : includes){
            count++;
            itemID = include.getItemID();
            ingredientID = include.getIngredientID();
            sql = String.format("insert into Includes (itemID, ingredientId) " +
                    "values (%d, %d);", itemID, ingredientID);
            try{
                search_sql = String.format("select * from Includes I where I.itemID=%d and I.ingredientID=%d;",itemID, ingredientID);
                rs = st.executeQuery(search_sql);
                if(!rs.next())
                    st.executeUpdate(sql);
            } catch (SQLException e){
                printException(e);
            }
        }
        return count;
    }

    @Override
    public int insertDietaryCategories(DietaryCategory[] categories) {
        int ingredientID;
        String dietaryCategory;

        int count = 0;
        String sql, search_sql;
        ResultSet rs;

        for (DietaryCategory category : categories){
            count++;
            ingredientID = category.getIngredientID();
            dietaryCategory = category.getDietaryCategory();
            sql = String.format("insert into DietaryCategories (ingredientID, dietaryCategory) " +
                    "values (%d, \"%s\");", ingredientID, dietaryCategory);
            try{
                search_sql = String.format("select * from DietaryCategories DC " +
                        "where DC.ingredientID=%d and DC.dietaryCategory= \"%s\";", ingredientID, dietaryCategory);
                rs = st.executeQuery(search_sql);
                if(!rs.next())
                    st.executeUpdate(sql);
            } catch (SQLException e){
                printException(e);
            }
        }
        return count;
    }

    @Override
    public int insertRatings(Rating[] ratings) {
        int ratingID;
        int itemID;
        int rating;
        String ratingDate;

        int count = 0;
        String sql, search_sql;
        ResultSet rs;

        for (Rating itRating : ratings){
            count++;
            ratingID = itRating.getRatingID();
            itemID = itRating.getItemID();
            rating = itRating.getRating();
            ratingDate = itRating.getRatingDate();
            sql = String.format("insert into Ratings (ratingID, itemID,rating, ratingDate) " +
                    "values (%d, %d, %d, \"%s\");", ratingID, itemID, rating, ratingDate);
            try{
                search_sql = String.format("select * from Ratings R where R.ratingID=%d;", ratingID);
                rs = st.executeQuery(search_sql);
                if(!rs.next())
                    st.executeUpdate(sql);
            } catch (SQLException e){
                printException(e);
            }
        }

        return count;
    }

    @Override
    public MenuItem[] getMenuItemsWithGivenIngredient(String name) {
        String query = String.format("select * from MenuItems MI, Includes Inc, Ingredients I " +
                "where MI.itemID=Inc.itemID and I.ingredientID=Inc.ingredientID and " +
                "I.ingredientName=\"%s\" order by MI.itemID;", name);
        ResultSet rs;

        ArrayList<MenuItem> items = new ArrayList<>();
        try{
            rs = st.executeQuery(query);

            while(rs.next()){
                int itemID = rs.getInt("itemID");
                String itemName = rs.getString("itemName");
                String cuisine = rs.getString("cuisine");
                int price = rs.getInt("price");
                MenuItem item = new MenuItem(itemID, itemName, cuisine, price);
                items.add(item);
            }
        } catch (SQLException e){
            printException(e);
        }

        MenuItem[] menuItemsWithGivenIngredients = new MenuItem[items.size()];
        items.toArray(menuItemsWithGivenIngredients);
        return menuItemsWithGivenIngredients;
    }

    @Override
    public MenuItem[] getMenuItemsWithoutAnyIngredient() {
        String query = "select * from MenuItems MI " +
                       "where not exists(select * from Includes I where " +
                       "MI.itemID=I.itemID);";
        ResultSet rs;

        ArrayList<MenuItem> items = new ArrayList<>();
        try{
            rs = st.executeQuery(query);
            while(rs.next()){
                int itemID = rs.getInt("itemID");
                String itemName = rs.getString("itemName");
                String cuisine = rs.getString("cuisine");
                int price = rs.getInt("price");
                MenuItem item = new MenuItem(itemID, itemName, cuisine, price);
                items.add(item);
            }
        } catch (SQLException e){
            printException(e);
        }

        MenuItem[] menuItemsWithoutGivenIngredients = new MenuItem[items.size()];
        items.toArray(menuItemsWithoutGivenIngredients);
        return menuItemsWithoutGivenIngredients;
    }

    @Override
    public Ingredient[] getNotIncludedIngredients() {
        String query = "select distinct Ing.ingredientID, Ing.ingredientName from Ingredients Ing " +
                "where not exists(select * from Includes I where " +
                "Ing.ingredientID=I.ingredientID) " +
                "order by Ing.ingredientID;";
        ResultSet rs;

        ArrayList<Ingredient> ingredients = new ArrayList<>();
        try{
            rs = st.executeQuery(query);
            while(rs.next()){
                int ingredientID = rs.getInt("ingredientID");
                String ingredientName = rs.getString("ingredientName");
                Ingredient ingredient = new Ingredient(ingredientID, ingredientName);
                ingredients.add(ingredient);
            }
        } catch (SQLException e){
            printException(e);
        }

        Ingredient[] notIncludedIngredients = new Ingredient[ingredients.size()];
        ingredients.toArray(notIncludedIngredients);
        return notIncludedIngredients;
    }

    @Override
    public MenuItem getMenuItemWithMostIngredients() {
        String query = "select distinct I.itemID, MI.itemName, MI.cuisine, MI.price " +
                        "from Includes I, MenuItems MI " +
                        "where MI.itemID = I.itemID group by I.itemID order by count(*) desc;";
        int itemID = 0, price = 0;
        String itemName = null, cuisine = null;
        try{
            ResultSet rs = st.executeQuery(query);
            rs.next();

            itemID = rs.getInt("itemID");
            itemName = rs.getString("itemName");
            cuisine = rs.getString("cuisine");
            price = rs.getInt("price");

        } catch (SQLException e){
            printException(e);
        }

        return new MenuItem(itemID, itemName, cuisine, price);
    }

    @Override
    public QueryResult.MenuItemAverageRatingResult[] getMenuItemsWithAvgRatings() {
        String query = "select distinct R.itemID, MI.itemName, avg(R.rating) as avgRating from " +
                "MenuItems MI, Ratings R where R.itemID=MI.itemID " +
                "group by R.itemID order by avgRating desc;";
        ResultSet rs;
        ArrayList<QueryResult.MenuItemAverageRatingResult> results = new ArrayList<>();

        try{
            rs = st.executeQuery(query);

            while(rs.next()){
                String itemID = rs.getString("itemID");
                String itemName = rs.getString("itemName");
                String avgRating = rs.getString("avgRating");
                QueryResult.MenuItemAverageRatingResult result = new QueryResult.MenuItemAverageRatingResult(itemID, itemName, avgRating);
                results.add(result);
            }
        } catch (SQLException e){
            printException(e);
        }

        QueryResult.MenuItemAverageRatingResult[] menuItemsWithAvgRatings = new QueryResult.MenuItemAverageRatingResult[results.size()];
        results.toArray(menuItemsWithAvgRatings);
        return menuItemsWithAvgRatings;
    }

    @Override
    public MenuItem[] getMenuItemsForDietaryCategory(String category) {
        String query = String.format(
                "select distinct MI.itemID, MI.itemName, MI.cuisine, MI.price " +
                        "from MenuItems MI, Includes I " +
                        "where I.itemID = MI.itemID and " +
                        "exists (select * from DietaryCategories DC where DC.ingredientID=I.ingredientID " +
                        "and DC.dietaryCategory=\"%s\")" +
                        "order by MI.itemID;",
                category);
        ResultSet rs;
        ArrayList<MenuItem> items = new ArrayList<>();
        try{
            rs = st.executeQuery(query);

            while(rs.next()){
                int itemID = rs.getInt("itemID");
                String itemName = rs.getString("itemName");
                String cuisine = rs.getString("cuisine");
                int price = rs.getInt("price");
                MenuItem item = new MenuItem(itemID, itemName, cuisine, price);
                items.add(item);
            }
        } catch (SQLException e){
            printException(e);
        }


        MenuItem[] menuItemsForGivenDietaryCategory = new MenuItem[items.size()];
        items.toArray(menuItemsForGivenDietaryCategory);

        return menuItemsForGivenDietaryCategory;
    }

    @Override
    public Ingredient getMostUsedIngredient() {
        String query = "select Inc.ingredientID, Ing.ingredientName from Ingredients Ing, Includes Inc " +
                    "where Ing.ingredientID=Inc.ingredientID group by Inc.ingredientID " +
                    "order by count(*) desc;";
        ResultSet rs;
        try{
            rs = st.executeQuery(query);
            rs.next();

            int ingredientID = rs.getInt("ingredientID");
            String ingredientName = rs.getString("ingredientName");

            return new Ingredient(ingredientID, ingredientName);
        } catch (SQLException e){
            printException(e);
        }

        return null;
    }

    @Override
    public QueryResult.CuisineWithAverageResult[] getCuisinesWithAvgRating() {
        String query = "select distinct MI.cuisine, avg(R.rating) as averageRating from MenuItems MI, Ratings R " +
                       "where R.itemID=MI.itemID and R.rating > 0 group by MI.cuisine " +
                       "order by averageRating desc;";

        String unratedQuery = "select distinct cuisine from MenuItems where cuisine not in " +
                              "(select MI.cuisine from MenuItems MI, Ratings R where R.itemID=MI.itemID and R.rating > 0 " +
                              "group by MI.cuisine);";

        ResultSet rs;
        ArrayList<QueryResult.CuisineWithAverageResult> cuisineRatings = new ArrayList<>();

        try{
            rs = st.executeQuery(query);
            String cuisine;
            String rating;

            while(rs.next()){
                cuisine = rs.getString("cuisine");
                rating = rs.getString("averageRating");
                QueryResult.CuisineWithAverageResult result = new QueryResult.CuisineWithAverageResult(cuisine, rating);
                cuisineRatings.add(result);
            }

            rs = st.executeQuery(unratedQuery);

            while(rs.next()){
                cuisine = rs.getString("cuisine");
                QueryResult.CuisineWithAverageResult unratedCuisine = new QueryResult.CuisineWithAverageResult(cuisine, "null");
                cuisineRatings.add(unratedCuisine);
            }

        } catch (SQLException e) {
            printException(e);
        }
        QueryResult.CuisineWithAverageResult[] cuisineWithAverageResults = new QueryResult.CuisineWithAverageResult[cuisineRatings.size()];
        cuisineRatings.toArray(cuisineWithAverageResults);

        return cuisineWithAverageResults;
    }

    @Override
    public QueryResult.CuisineWithAverageResult[] getCuisinesWithAvgIngredientCount() {
        String query = "select T.cuisine, avg(T.counts) as avgCount " +
                        "from (select MI.cuisine, count(I.itemID) as counts " +
                                "from MenuItems MI, Includes I where I.itemID=MI.itemID " +
                                "group by MI.cuisine, MI.itemID) as T " +
                        "group by T.cuisine order by avgCount desc;";

        String notIncludedCuisines = "select distinct cuisine from MenuItems " +
                                    "where cuisine not in (select MI.cuisine " +
                                    "from Includes I, MenuItems MI where I.itemID=MI.itemID);";

        ResultSet rs;
        ArrayList<QueryResult.CuisineWithAverageResult> results = new ArrayList<>();

        try{
            rs = st.executeQuery(query);
            String cuisine;
            String average;

            while(rs.next()){
                cuisine = rs.getString("cuisine");
                average = rs.getString("avgCount");
                QueryResult.CuisineWithAverageResult result = new QueryResult.CuisineWithAverageResult(cuisine, average);
                results.add(result);
            }

            rs = st.executeQuery(notIncludedCuisines);
            while(rs.next()){
                cuisine = rs.getString("cuisine");
                results.add(new QueryResult.CuisineWithAverageResult(cuisine, "0.0000"));
            }

        } catch (SQLException e){
            printException(e);
        }
        QueryResult.CuisineWithAverageResult[] cuisineWithAverageResults = new QueryResult.CuisineWithAverageResult[results.size()];
        results.toArray(cuisineWithAverageResults);

        return cuisineWithAverageResults;
    }

    @Override
    public int increasePrice(String ingredientName, String increaseAmount) {
        int increase = Integer.parseInt(increaseAmount);
        String query = String.format("select MI.itemID, MI.price from MenuItems MI, Ingredients Ing, Includes Inc " +
                                     "where Inc.itemID=MI.itemID and Ing.ingredientName=\"%s\" and " +
                                     "Inc.ingredientID=Ing.ingredientID;", ingredientName);
        ResultSet rs;

        int numberOfRowsAffected=0;
        try{
            rs = st.executeQuery(query);
            int itemID, price, newPrice;
            String set_price_query;
            Statement set_price_st = connection.createStatement();

            while (rs.next()){
                itemID = rs.getInt("itemID");
                price = rs.getInt("price");
                newPrice = price + increase;
                set_price_query = String.format("update MenuItems set price=%d where itemID=%d;", newPrice, itemID);
                set_price_st.executeUpdate(set_price_query);
                numberOfRowsAffected++;
            }

        } catch (SQLException e){
            printException(e);
        }

        return numberOfRowsAffected;
    }

    @Override
    public Rating[] deleteOlderRatings(String date) {
        String query = String.format("select * from Ratings where ratingDate < \"%s\" order by ratingID;", date);
        ResultSet rs;
        ArrayList<Rating> olderRatings = new ArrayList<>();

        try{
            rs = st.executeQuery(query);
            int ratingID, itemID, rating;
            String dateOfRating;

            while(rs.next()){
                ratingID = rs.getInt("ratingID");
                itemID = rs.getInt("itemID");
                rating = rs.getInt("rating");
                dateOfRating = String.valueOf(rs.getDate("ratingDate"));
                Rating olderRating = new Rating(ratingID, itemID, rating, dateOfRating);
                olderRatings.add(olderRating);
            }
            String delete_query = String.format("delete from Ratings where ratingDate < \"%s\";", date);
            st.executeUpdate(delete_query);

        } catch (SQLException e){
            printException(e);
        }

        Rating[] deletedRatings = new Rating[olderRatings.size()];
        olderRatings.toArray(deletedRatings);

        return deletedRatings;
    }

    private static void printException(SQLException ex) {
        System.out.println("My Exception: " + ex.getMessage() + "\n");
    }
}